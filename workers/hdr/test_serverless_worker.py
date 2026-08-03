from __future__ import annotations

import hashlib
import hmac
import io
import json
import socket
import ssl
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Callable
from unittest.mock import patch

from PIL import Image

from serverless_worker import (
    CorePipelineRunner,
    FakeObjectTransport,
    HdrWorker,
    HttpsObjectTransport,
    WorkerConfig,
    WorkerError,
    canonical_json,
    dispatch_signature_payload,
)


TENANT = "tenant-a"
PROJECT = "project-a"
GROUP = "group-a"
JOB = "job-1234567890"
SECRET = b"test-only-worker-secret-that-is-long-enough"
HOST = "unit-test.r2.example"


def jpeg_bytes(width: int = 12, height: int = 8) -> bytes:
    stream = io.BytesIO()
    Image.new("RGB", (width, height), (40, 80, 120)).save(stream, format="JPEG")
    return stream.getvalue()


def url(key: str, operation: str) -> str:
    return f"https://{HOST}/private/{key}?grant={operation}"


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def signed_dispatch(manifest: dict, *, expires_at: int = 2_000_000) -> tuple[dict, bytes]:
    manifest_bytes = canonical_json(manifest)
    attempt = manifest["job"]["attempt"]
    dispatch = {
        "version": "hdr-dispatch.v1",
        "audience": "runpod-hdr",
        "jobId": JOB,
        "tenantId": TENANT,
        "projectId": PROJECT,
        "groupId": GROUP,
        "manifestObjectKey": f"users/{TENANT}/projects/{PROJECT}/jobs/{JOB}/manifest-{attempt}.json",
        "manifestUrl": url(f"users/{TENANT}/projects/{PROJECT}/jobs/{JOB}/manifest-{attempt}.json", "get"),
        "manifestSha256": sha(manifest_bytes),
        "expiresAt": expires_at,
    }
    dispatch["signature"] = hmac.new(
        SECRET,
        dispatch_signature_payload(dispatch),
        hashlib.sha256,
    ).hexdigest()
    return dispatch, manifest_bytes


def manifest(*, attempt: int = 1, max_attempts: int = 3) -> dict:
    base = f"users/{TENANT}/projects/{PROJECT}"
    input_a = b"fake-raw-a"
    input_b = b"fake-raw-b"
    input_c = b"fake-raw-c"
    return {
        "version": "hdr-worker-manifest.v1",
        "job": {
            "jobId": JOB,
            "tenantId": TENANT,
            "projectId": PROJECT,
            "groupId": GROUP,
            "attempt": attempt,
        },
        "createdAt": 1_000_000,
        "expiresAt": 2_000_000,
        "inputs": [
            {
                "objectKey": f"{base}/originals/a.CR3",
                "versionId": "version-a",
                "filename": "a.CR3",
                "bytes": len(input_a),
                "sha256": sha(input_a),
                "width": 12,
                "height": 8,
                "exposureBias": -2,
                "downloadUrl": url(f"{base}/originals/a.CR3", "get"),
                "urlExpiresAt": 2_000_000,
            },
            {
                "objectKey": f"{base}/originals/b.CR3",
                "versionId": "version-b",
                "filename": "b.CR3",
                "bytes": len(input_b),
                "sha256": sha(input_b),
                "width": 12,
                "height": 8,
                "exposureBias": 0,
                "downloadUrl": url(f"{base}/originals/b.CR3", "get"),
                "urlExpiresAt": 2_000_000,
            },
            {
                "objectKey": f"{base}/originals/c.CR3",
                "versionId": "version-c",
                "filename": "c.CR3",
                "bytes": len(input_c),
                "sha256": sha(input_c),
                "width": 12,
                "height": 8,
                "exposureBias": 2,
                "downloadUrl": url(f"{base}/originals/c.CR3", "get"),
                "urlExpiresAt": 2_000_000,
            },
        ],
        "output": {
            "format": "jpeg",
            "preserveInputDimensions": True,
            "width": 12,
            "height": 8,
            "objectKey": f"{base}/intermediates/{GROUP}-hdr.jpg",
            "downloadUrl": url(f"{base}/intermediates/{GROUP}-hdr.jpg", "get"),
            "uploadUrl": url(f"{base}/intermediates/{GROUP}-hdr.jpg", "put"),
            "metadataObjectKey": f"{base}/intermediates/{GROUP}-hdr.evidence.json",
            "metadataDownloadUrl": url(f"{base}/intermediates/{GROUP}-hdr.evidence.json", "get"),
            "metadataUploadUrl": url(f"{base}/intermediates/{GROUP}-hdr.evidence.json", "put"),
            "urlExpiresAt": 2_000_000,
        },
        "checkpoint": {
            "currentObjectKey": f"{base}/jobs/{JOB}/checkpoints/current.tar.gz",
            "currentDownloadUrl": url(f"{base}/jobs/{JOB}/checkpoints/current.tar.gz", "get"),
            "currentUploadUrl": url(f"{base}/jobs/{JOB}/checkpoints/current.tar.gz", "put"),
            "previousObjectKey": f"{base}/jobs/{JOB}/checkpoints/previous.tar.gz",
            "previousDownloadUrl": url(f"{base}/jobs/{JOB}/checkpoints/previous.tar.gz", "get"),
            "previousUploadUrl": url(f"{base}/jobs/{JOB}/checkpoints/previous.tar.gz", "put"),
            "urlExpiresAt": 2_000_000,
        },
        "completion": {
            "objectKey": f"{base}/jobs/{JOB}/completion.json",
            "downloadUrl": url(f"{base}/jobs/{JOB}/completion.json", "get"),
            "uploadUrl": url(f"{base}/jobs/{JOB}/completion.json", "put"),
            "urlExpiresAt": 2_000_000,
        },
        "failure": {
            "objectKey": f"{base}/jobs/{JOB}/failures/{attempt}.json",
            "uploadUrl": url(f"{base}/jobs/{JOB}/failures/{attempt}.json", "put"),
            "urlExpiresAt": 2_000_000,
        },
        "limits": {
            "cpuCores": 1,
            "memoryBytes": 512 * 1024 * 1024,
            "scratchBytes": 64 * 1024 * 1024,
            "maxInputBytes": 1024,
            "maxOutputBytes": 1024 * 1024,
            "maxPixels": 1000,
            "maxDecompressedBytes": 1024 * 1024,
            "executionMs": 30_000,
            "maxAttempts": max_attempts,
        },
    }


def seed_manifest_inputs(transport: FakeObjectTransport, value: dict) -> None:
    payloads = (b"fake-raw-a", b"fake-raw-b", b"fake-raw-c")
    for item, payload in zip(value["inputs"], payloads):
        transport.seed(item["downloadUrl"], payload)


def config(root: Path) -> WorkerConfig:
    return WorkerConfig(
        enabled=True,
        manifest_secret=SECRET,
        allowed_hosts=frozenset({HOST}),
        work_root=root,
        max_cpu_cores=2,
        max_memory_bytes=2 * 1024 * 1024 * 1024,
        max_scratch_bytes=256 * 1024 * 1024,
        max_input_bytes=1024 * 1024,
        max_output_bytes=8 * 1024 * 1024,
        max_pixels=10_000,
        max_decompressed_bytes=16 * 1024 * 1024,
        max_execution_ms=60_000,
        max_attempts=3,
        checkpoint_archive_bytes=32 * 1024 * 1024,
    )


class FakeRunner:
    def __init__(self) -> None:
        self.calls = 0

    def run(self, sources: list[Path], output_dir: Path, validated: dict, deadline: float) -> dict:
        self.calls += 1
        self.assert_sources_are_job_local(sources, output_dir)
        checkpoint_dir = output_dir / "checkpoints"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        (checkpoint_dir / "merge_state.json").write_text('{"next_row":8}', encoding="utf-8")
        rendered = output_dir / "fixture_HDR_fullres.jpg"
        rendered.write_bytes(jpeg_bytes())
        return {
            "status": "complete",
            "pipeline": "fixture",
            "render": {"jpeg": str(rendered)},
            "quality_gates": {"output_dimensions_preserved": True},
        }

    @staticmethod
    def assert_sources_are_job_local(sources: list[Path], output_dir: Path) -> None:
        root = output_dir.parent.resolve()
        assert all(path.resolve().is_relative_to(root) for path in sources)


class HttpsObjectTransportTests(unittest.TestCase):
    def test_network_failures_are_normalized_without_raw_details(self) -> None:
        cases = [
            (socket.gaierror("sensitive host"), "OBJECT_DNS_FAILED", True),
            (ssl.SSLError("certificate detail"), "OBJECT_TLS_FAILED", False),
            (TimeoutError("signed url"), "OBJECT_TIMEOUT", True),
            (OSError("local path"), "OBJECT_NETWORK_FAILED", True),
        ]
        for error, code, retryable in cases:
            with self.subTest(code=code):
                normalized = HttpsObjectTransport._normalized_network_error(error)
                self.assertEqual(normalized.code, code)
                self.assertEqual(normalized.retryable, retryable)
                self.assertNotIn(str(error), str(normalized))

    def test_short_read_is_rejected_and_partial_file_removed(self) -> None:
        class Response:
            status = 200

            def __init__(self) -> None:
                self._reads = [b"abc", b""]

            def getheader(self, name: str) -> str | None:
                return "5" if name == "Content-Length" else None

            def read(self, _size: int = -1) -> bytes:
                return self._reads.pop(0)

        class Connection:
            def __init__(self, *_args, **_kwargs) -> None:
                self.response = Response()

            def request(self, *_args, **_kwargs) -> None:
                return None

            def getresponse(self) -> Response:
                return self.response

            def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as temporary, patch("serverless_worker.http.client.HTTPSConnection", Connection):
            destination = Path(temporary) / "partial.bin"
            with self.assertRaises(WorkerError) as raised:
                HttpsObjectTransport().download("https://unit-test.example/private/a", destination, 10)
            self.assertEqual(raised.exception.code, "OBJECT_SHORT_READ")
            self.assertTrue(raised.exception.retryable)
            self.assertFalse(destination.exists())

    def test_invalid_content_length_is_rejected_safely(self) -> None:
        class Response:
            status = 200

            def getheader(self, _name: str) -> str:
                return "not-a-number"

            def read(self, _size: int = -1) -> bytes:
                return b""

        class Connection:
            def __init__(self, *_args, **_kwargs) -> None:
                pass

            def request(self, *_args, **_kwargs) -> None:
                return None

            def getresponse(self) -> Response:
                return Response()

            def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as temporary, patch("serverless_worker.http.client.HTTPSConnection", Connection):
            with self.assertRaises(WorkerError) as raised:
                HttpsObjectTransport().download("https://unit-test.example/private/a", Path(temporary) / "a", 10)
            self.assertEqual(raised.exception.code, "OBJECT_LENGTH_INVALID")

    def test_download_refreshes_socket_timeout_from_total_deadline_for_each_read(self) -> None:
        class Socket:
            def __init__(self) -> None:
                self.timeouts: list[float] = []

            def settimeout(self, value: float) -> None:
                self.timeouts.append(value)

        class Response:
            status = 200

            def __init__(self) -> None:
                self._reads = [b"abc", b""]

            def getheader(self, _name: str) -> None:
                return None

            def read1(self, _size: int) -> bytes:
                return self._reads.pop(0)

        class Connection:
            last: "Connection | None" = None

            def __init__(self, *_args, **_kwargs) -> None:
                self.sock = Socket()
                Connection.last = self

            def request(self, *_args, **_kwargs) -> None:
                return None

            def getresponse(self) -> Response:
                return Response()

            def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as temporary, \
                patch("serverless_worker.http.client.HTTPSConnection", Connection), \
                patch("serverless_worker.time.monotonic", side_effect=[100.0, 101.0, 102.0, 103.0]):
            destination = Path(temporary) / "complete.bin"
            HttpsObjectTransport(timeout_seconds=60).download(
                "https://unit-test.example/private/a", destination, 10, deadline=110.0
            )
            self.assertEqual(destination.read_bytes(), b"abc")
            self.assertEqual(Connection.last.sock.timeouts, [9.0, 8.0, 7.0])

    def test_upload_checks_total_deadline_between_chunks(self) -> None:
        class Socket:
            def __init__(self) -> None:
                self.timeouts: list[float] = []

            def settimeout(self, value: float) -> None:
                self.timeouts.append(value)

        class Response:
            status = 200

            def read(self, _size: int) -> bytes:
                return b""

        class Connection:
            last: "Connection | None" = None

            def __init__(self, *_args, **_kwargs) -> None:
                self.sock = Socket()
                self.sent: list[bytes] = []
                Connection.last = self

            def putrequest(self, *_args, **_kwargs) -> None:
                return None

            def putheader(self, *_args, **_kwargs) -> None:
                return None

            def endheaders(self) -> None:
                return None

            def send(self, block: bytes) -> None:
                self.sent.append(block)

            def getresponse(self) -> Response:
                return Response()

            def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as temporary, \
                patch("serverless_worker.http.client.HTTPSConnection", Connection), \
                patch("serverless_worker.time.monotonic", side_effect=[100.0, 101.0, 102.0, 103.0, 104.0, 105.0]):
            source = Path(temporary) / "output.jpg"
            source.write_bytes(b"a" * (1024 * 1024 + 1))
            HttpsObjectTransport(timeout_seconds=60).upload(
                "https://unit-test.example/private/a", source, source.stat().st_size, "image/jpeg", deadline=110.0
            )
            self.assertEqual(sum(map(len, Connection.last.sent)), source.stat().st_size)
            self.assertEqual(Connection.last.sock.timeouts, [9.0, 8.0, 7.0, 6.0, 5.0])


class ServerlessWorkerTests(unittest.TestCase):
    def test_total_deadline_starts_before_manifest_download(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            value = manifest()
            dispatch, manifest_bytes = signed_dispatch(value)
            transport = FakeObjectTransport()
            transport.seed(dispatch["manifestUrl"], manifest_bytes)
            constrained = replace(config(Path(temporary)), max_execution_ms=1_000)
            worker = HdrWorker(constrained, transport=transport, runner=FakeRunner(), now_ms=lambda: 1_500_000)
            with patch("serverless_worker.time.monotonic", side_effect=[10.0, 11.1]):
                with self.assertRaises(WorkerError) as raised:
                    worker.process(dispatch)
            self.assertEqual(raised.exception.code, "PROCESS_TIMEOUT")
            self.assertTrue(raised.exception.retryable)

    def test_core_runner_is_bound_to_the_validated_worker_resource_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = root / "fixture.py"
            script.write_text('import json\nprint(json.dumps({"status": "complete"}))\n', encoding="utf-8")
            worker_config = config(root / "scratch")
            runner = CorePipelineRunner(worker_config, script=script)
            self.assertIs(runner.config, worker_config)

    def test_end_to_end_fake_transport_is_idempotent_and_returns_no_urls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = manifest()
            dispatch, manifest_bytes = signed_dispatch(value)
            transport = FakeObjectTransport()
            transport.seed(dispatch["manifestUrl"], manifest_bytes)
            seed_manifest_inputs(transport, value)
            runner = FakeRunner()
            worker = HdrWorker(config(root), transport=transport, runner=runner, now_ms=lambda: 1_500_000)

            first = worker.process(dispatch)
            second = worker.process(dispatch)

            self.assertEqual(first["status"], "completed")
            self.assertEqual(first["output"]["width"], 12)
            self.assertEqual(first["output"]["height"], 8)
            self.assertEqual(second["idempotentReplay"], True)
            self.assertTrue(worker._verify_signed_record(first))
            self.assertTrue(worker._verify_signed_record(second))
            self.assertEqual(runner.calls, 1)
            self.assertNotIn("url", json.dumps(first).lower())
            self.assertFalse(any(root.iterdir()), "per-job scratch data must be removed")

    def test_completion_replay_requires_the_exact_dispatch_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            first_manifest = manifest(attempt=1)
            first_dispatch, first_manifest_bytes = signed_dispatch(first_manifest)
            transport = FakeObjectTransport()
            transport.seed(first_dispatch["manifestUrl"], first_manifest_bytes)
            seed_manifest_inputs(transport, first_manifest)
            runner = FakeRunner()
            worker = HdrWorker(config(Path(temporary)), transport=transport, runner=runner, now_ms=lambda: 1_500_000)
            worker.process(first_dispatch)

            second_manifest = manifest(attempt=2)
            second_dispatch, second_manifest_bytes = signed_dispatch(second_manifest)
            transport.seed(second_dispatch["manifestUrl"], second_manifest_bytes)
            seed_manifest_inputs(transport, second_manifest)
            with self.assertRaises(WorkerError) as raised:
                worker.process(second_dispatch)

            self.assertEqual(raised.exception.code, "COMPLETION_INVALID")
            self.assertEqual(runner.calls, 1)

    def test_output_evidence_repairs_a_lost_completion_marker_without_rerunning(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            value = manifest()
            dispatch, manifest_bytes = signed_dispatch(value)
            transport = FakeObjectTransport()
            transport.seed(dispatch["manifestUrl"], manifest_bytes)
            seed_manifest_inputs(transport, value)
            runner = FakeRunner()
            worker = HdrWorker(config(Path(temporary)), transport=transport, runner=runner, now_ms=lambda: 1_500_000)

            worker.process(dispatch)
            transport.remove(value["completion"]["downloadUrl"])
            replay = worker.process(dispatch)

            self.assertEqual(replay["idempotentReplay"], True)
            self.assertTrue(worker._verify_signed_record(replay))
            self.assertEqual(runner.calls, 1)
            self.assertIn(value["completion"]["uploadUrl"], transport.uploaded)

    def test_output_evidence_must_bind_the_exact_input_versions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            value = manifest()
            dispatch, manifest_bytes = signed_dispatch(value)
            transport = FakeObjectTransport()
            transport.seed(dispatch["manifestUrl"], manifest_bytes)
            seed_manifest_inputs(transport, value)
            worker = HdrWorker(config(Path(temporary)), transport=transport, runner=FakeRunner(), now_ms=lambda: 1_500_000)

            worker.process(dispatch)
            evidence = json.loads(transport.uploaded[value["output"]["metadataUploadUrl"]])
            evidence.pop("evidenceSignature")
            evidence["inputs"][0]["versionId"] = "different-version"
            evidence = worker._sign_record(evidence)
            transport.remove(value["completion"]["downloadUrl"])
            transport.seed(value["output"]["metadataDownloadUrl"], canonical_json(evidence))

            with self.assertRaises(WorkerError) as raised:
                worker.process(dispatch)
            self.assertEqual(raised.exception.code, "COMPLETION_INVALID")

    def test_rejects_bad_signature_expiry_cross_tenant_and_non_allowlisted_urls(self) -> None:
        cases: list[tuple[str, Callable[[dict, dict], None]]] = [
            ("AUTH_INVALID", lambda d, m: d.update(signature="0" * 64)),
            ("AUTH_EXPIRED", lambda d, m: d.update(expiresAt=1_400_000)),
            ("IDENTITY_MISMATCH", lambda d, m: m["job"].update(tenantId="tenant-b")),
            ("URL_NOT_ALLOWED", lambda d, m: m["inputs"][0].update(downloadUrl="https://169.254.169.254/latest/meta-data")),
        ]
        for code, mutate in cases:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as temporary:
                value = manifest()
                dispatch, manifest_bytes = signed_dispatch(value)
                mutate(dispatch, value)
                if code in {"AUTH_EXPIRED", "IDENTITY_MISMATCH", "URL_NOT_ALLOWED"}:
                    dispatch, manifest_bytes = signed_dispatch(value)
                    if code == "AUTH_EXPIRED":
                        dispatch, manifest_bytes = signed_dispatch(value, expires_at=1_400_000)
                transport = FakeObjectTransport()
                transport.seed(dispatch["manifestUrl"], manifest_bytes)
                worker = HdrWorker(config(Path(temporary)), transport=transport, runner=FakeRunner(), now_ms=lambda: 1_500_000)
                with self.assertRaises(WorkerError) as raised:
                    worker.process(dispatch)
                self.assertEqual(raised.exception.code, code)

    def test_rejects_content_mismatch_same_exposure_and_dimension_drift(self) -> None:
        for code, mutate in [
            ("INPUT_INTEGRITY", lambda m: m["inputs"][0].update(sha256="f" * 64)),
            ("EXPOSURE_INVALID", lambda m: [item.update(exposureBias=0) for item in m["inputs"]]),
            ("EXPOSURE_INVALID", lambda m: m["inputs"][0].update(exposureBias=float("nan"))),
            ("DIMENSION_MISMATCH", lambda m: m["inputs"][1].update(width=11)),
        ]:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as temporary:
                value = manifest()
                mutate(value)
                dispatch, manifest_bytes = signed_dispatch(value)
                transport = FakeObjectTransport()
                transport.seed(dispatch["manifestUrl"], manifest_bytes)
                seed_manifest_inputs(transport, value)
                worker = HdrWorker(config(Path(temporary)), transport=transport, runner=FakeRunner(), now_ms=lambda: 1_500_000)
                with self.assertRaises(WorkerError) as raised:
                    worker.process(dispatch)
                self.assertEqual(raised.exception.code, code)

    def test_manifest_object_key_must_match_the_signed_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            value = manifest()
            dispatch, manifest_bytes = signed_dispatch(value)
            mismatched_key = f"users/{TENANT}/projects/{PROJECT}/jobs/{JOB}/manifest-2.json"
            dispatch["manifestObjectKey"] = mismatched_key
            dispatch["manifestUrl"] = url(mismatched_key, "get")
            dispatch["signature"] = hmac.new(SECRET, dispatch_signature_payload(dispatch), hashlib.sha256).hexdigest()
            transport = FakeObjectTransport()
            transport.seed(dispatch["manifestUrl"], manifest_bytes)
            worker = HdrWorker(config(Path(temporary)), transport=transport, runner=FakeRunner(), now_ms=lambda: 1_500_000)
            with self.assertRaises(WorkerError) as raised:
                worker.process(dispatch)
            self.assertEqual(raised.exception.code, "IDENTITY_MISMATCH")

    def test_rejects_wrong_job_object_paths_invalid_versions_and_duplicate_objects(self) -> None:
        cases: list[tuple[str, Callable[[dict], None]]] = [
            ("IDENTITY_MISMATCH", lambda m: m["checkpoint"].update(currentObjectKey=f"users/{TENANT}/projects/{PROJECT}/jobs/other-job/checkpoints/current.tar.gz")),
            ("INPUT_INVALID", lambda m: m["inputs"][0].update(versionId="bad/version")),
            ("INPUT_INVALID", lambda m: m["inputs"][1].update(objectKey=m["inputs"][0]["objectKey"], versionId=m["inputs"][0]["versionId"])),
            ("INPUT_INVALID", lambda m: m.update(inputs=m["inputs"][:1])),
        ]
        for code, mutate in cases:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as temporary:
                value = manifest()
                mutate(value)
                dispatch, manifest_bytes = signed_dispatch(value)
                transport = FakeObjectTransport()
                transport.seed(dispatch["manifestUrl"], manifest_bytes)
                worker = HdrWorker(config(Path(temporary)), transport=transport, runner=FakeRunner(), now_ms=lambda: 1_500_000)
                with self.assertRaises(WorkerError) as raised:
                    worker.process(dispatch)
                self.assertEqual(raised.exception.code, code)

    def test_corrupt_current_checkpoint_falls_back_to_previous(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            value = manifest()
            dispatch, manifest_bytes = signed_dispatch(value)
            transport = FakeObjectTransport()
            transport.seed(dispatch["manifestUrl"], manifest_bytes)
            seed_manifest_inputs(transport, value)
            runner = FakeRunner()
            worker = HdrWorker(config(root), transport=transport, runner=runner, now_ms=lambda: 1_500_000)
            worker.process(dispatch)
            valid_checkpoint = transport.uploaded[value["checkpoint"]["currentUploadUrl"]]

            # Force a new attempt without the completion marker. The corrupt current
            # slot must be ignored and the previous valid archive restored.
            transport.remove(value["completion"]["downloadUrl"])
            transport.remove(value["output"]["metadataDownloadUrl"])
            transport.seed(value["checkpoint"]["currentDownloadUrl"], b"not-a-checkpoint")
            transport.seed(value["checkpoint"]["previousDownloadUrl"], valid_checkpoint)
            worker.process(dispatch)
            self.assertEqual(runner.calls, 2)
            self.assertEqual(worker.last_checkpoint_source, "previous")

    def test_checkpoint_from_changed_inputs_is_not_restored(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = manifest()
            dispatch, manifest_bytes = signed_dispatch(original)
            transport = FakeObjectTransport()
            transport.seed(dispatch["manifestUrl"], manifest_bytes)
            seed_manifest_inputs(transport, original)
            runner = FakeRunner()
            worker = HdrWorker(config(root), transport=transport, runner=runner, now_ms=lambda: 1_500_000)
            worker.process(dispatch)
            saved = transport.uploaded[original["checkpoint"]["currentUploadUrl"]]

            changed = manifest()
            changed_bytes = b"different-a"
            changed["inputs"][0].update(bytes=len(changed_bytes), sha256=sha(changed_bytes), versionId="version-a2")
            changed_dispatch, changed_manifest_bytes = signed_dispatch(changed)
            transport.seed(changed_dispatch["manifestUrl"], changed_manifest_bytes)
            transport.seed(changed["inputs"][0]["downloadUrl"], changed_bytes)
            transport.seed(changed["inputs"][1]["downloadUrl"], b"fake-raw-b")
            transport.seed(changed["inputs"][2]["downloadUrl"], b"fake-raw-c")
            transport.remove(changed["completion"]["downloadUrl"])
            transport.remove(changed["output"]["metadataDownloadUrl"])
            transport.seed(changed["checkpoint"]["currentDownloadUrl"], saved)

            worker.process(changed_dispatch)
            self.assertIsNone(worker.last_checkpoint_source)

    def test_bounded_failure_is_archived_without_sensitive_details(self) -> None:
        class FailingRunner(FakeRunner):
            def run(self, sources: list[Path], output_dir: Path, validated: dict, deadline: float) -> dict:
                checkpoint_dir = output_dir / "checkpoints"
                checkpoint_dir.mkdir(parents=True, exist_ok=True)
                (checkpoint_dir / "merge_state.json").write_text('{"next_row":4}', encoding="utf-8")
                raise WorkerError("PROCESS_TIMEOUT", retryable=True)

        with tempfile.TemporaryDirectory() as temporary:
            value = manifest(attempt=3, max_attempts=3)
            dispatch, manifest_bytes = signed_dispatch(value)
            transport = FakeObjectTransport()
            transport.seed(dispatch["manifestUrl"], manifest_bytes)
            seed_manifest_inputs(transport, value)
            worker = HdrWorker(config(Path(temporary)), transport=transport, runner=FailingRunner(), now_ms=lambda: 1_500_000)
            with self.assertRaises(WorkerError):
                worker.process(dispatch)
            failure = json.loads(transport.uploaded[value["failure"]["uploadUrl"]])
            self.assertEqual(failure["status"], "dead_letter")
            self.assertEqual(failure["code"], "PROCESS_TIMEOUT")
            self.assertIn(value["checkpoint"]["currentUploadUrl"], transport.uploaded)
            self.assertNotIn("http", json.dumps(failure).lower())
            self.assertNotIn("secret", json.dumps(failure).lower())

    def test_cancelled_job_is_archived_without_retrying(self) -> None:
        class CancelledRunner(FakeRunner):
            def run(self, sources: list[Path], output_dir: Path, validated: dict, deadline: float) -> dict:
                raise WorkerError("CANCELLED")

        with tempfile.TemporaryDirectory() as temporary:
            value = manifest()
            dispatch, manifest_bytes = signed_dispatch(value)
            transport = FakeObjectTransport()
            transport.seed(dispatch["manifestUrl"], manifest_bytes)
            seed_manifest_inputs(transport, value)
            worker = HdrWorker(config(Path(temporary)), transport=transport, runner=CancelledRunner(), now_ms=lambda: 1_500_000)
            with self.assertRaises(WorkerError):
                worker.process(dispatch)
            failure = json.loads(transport.uploaded[value["failure"]["uploadUrl"]])
            self.assertEqual(failure["status"], "cancelled")
            self.assertFalse(failure["retryable"])


if __name__ == "__main__":
    unittest.main()
