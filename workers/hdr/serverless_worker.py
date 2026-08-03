"""Restricted RunPod-facing HDR worker with signed object-storage manifests.

The control plane signs a short-lived dispatch. The worker never accepts local
paths or arbitrary output destinations from a browser. Photo bytes travel
between private object storage and this isolated worker, not through the web
application or its database.
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import io
import json
import math
import os
import re
import resource
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol
from urllib.parse import unquote, urlsplit

from PIL import Image


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,127}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SUPPORTED_INPUTS = {".cr2", ".cr3", ".nef", ".nrw", ".arw", ".raf"}


class WorkerError(RuntimeError):
    def __init__(self, code: str, *, retryable: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def dispatch_signature_payload(dispatch: dict[str, Any]) -> bytes:
    return canonical_json({key: value for key, value in dispatch.items() if key != "signature"})


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class WorkerConfig:
    enabled: bool
    manifest_secret: bytes
    allowed_hosts: frozenset[str]
    work_root: Path
    max_cpu_cores: int
    max_memory_bytes: int
    max_scratch_bytes: int
    max_input_bytes: int
    max_output_bytes: int
    max_pixels: int
    max_decompressed_bytes: int
    max_execution_ms: int
    max_attempts: int
    checkpoint_archive_bytes: int
    max_open_files: int = 256
    max_processes: int = 16
    object_path_prefix: str = "/private/"

    @classmethod
    def from_environment(cls) -> "WorkerConfig":
        secret = os.environ.get("HDR_WORKER_MANIFEST_SECRET", "").encode("utf-8")
        hosts = frozenset(part.strip().lower() for part in os.environ.get("HDR_WORKER_R2_HOSTS", "").split(",") if part.strip())
        return cls(
            enabled=os.environ.get("HDR_WORKER_ENABLED", "false").lower() == "true",
            manifest_secret=secret,
            allowed_hosts=hosts,
            work_root=Path(os.environ.get("HDR_WORKER_WORK_ROOT", "/tmp/jin-lumeo-hdr")),
            max_cpu_cores=int(os.environ.get("HDR_WORKER_MAX_CPU_CORES", "2")),
            max_memory_bytes=int(os.environ.get("HDR_WORKER_MAX_MEMORY_BYTES", str(8 * 1024**3))),
            max_scratch_bytes=int(os.environ.get("HDR_WORKER_MAX_SCRATCH_BYTES", str(24 * 1024**3))),
            max_input_bytes=int(os.environ.get("HDR_WORKER_MAX_INPUT_BYTES", str(4 * 1024**3))),
            max_output_bytes=int(os.environ.get("HDR_WORKER_MAX_OUTPUT_BYTES", str(2 * 1024**3))),
            max_pixels=int(os.environ.get("HDR_WORKER_MAX_PIXELS", "100000000")),
            max_decompressed_bytes=int(os.environ.get("HDR_WORKER_MAX_DECOMPRESSED_BYTES", str(16 * 1024**3))),
            max_execution_ms=int(os.environ.get("HDR_WORKER_MAX_EXECUTION_MS", str(45 * 60 * 1000))),
            max_attempts=int(os.environ.get("HDR_WORKER_MAX_ATTEMPTS", "3")),
            checkpoint_archive_bytes=int(os.environ.get("HDR_WORKER_MAX_CHECKPOINT_ARCHIVE_BYTES", str(16 * 1024**3))),
            max_open_files=int(os.environ.get("HDR_WORKER_MAX_OPEN_FILES", "256")),
            max_processes=int(os.environ.get("HDR_WORKER_MAX_PROCESSES", "16")),
            object_path_prefix=os.environ.get("HDR_WORKER_OBJECT_PATH_PREFIX", "/private/"),
        )

    def validate(self) -> None:
        if not self.enabled:
            raise WorkerError("WORKER_DISABLED")
        if len(self.manifest_secret) < 32:
            raise WorkerError("CONFIG_INVALID")
        if not self.allowed_hosts or any("/" in host or ":" in host for host in self.allowed_hosts):
            raise WorkerError("CONFIG_INVALID")
        numeric = [
            self.max_cpu_cores,
            self.max_memory_bytes,
            self.max_scratch_bytes,
            self.max_input_bytes,
            self.max_output_bytes,
            self.max_pixels,
            self.max_decompressed_bytes,
            self.max_execution_ms,
            self.max_attempts,
            self.checkpoint_archive_bytes,
            self.max_open_files,
            self.max_processes,
        ]
        if any(value <= 0 for value in numeric) or not self.object_path_prefix.startswith("/"):
            raise WorkerError("CONFIG_INVALID")


class ObjectTransport(Protocol):
    def download(self, url: str, destination: Path, max_bytes: int, *, optional: bool = False, deadline: float | None = None) -> bool: ...

    def upload(self, url: str, source: Path, max_bytes: int, content_type: str, *, deadline: float | None = None) -> None: ...


class HttpsObjectTransport:
    """Minimal no-redirect HTTPS transport suitable for presigned object URLs."""

    def __init__(self, timeout_seconds: float = 60.0) -> None:
        self.timeout_seconds = timeout_seconds

    @staticmethod
    def _parts(url: str) -> tuple[str, str]:
        parts = urlsplit(url)
        if parts.scheme != "https" or not parts.hostname or parts.port not in (None, 443):
            raise WorkerError("URL_NOT_ALLOWED")
        return parts.hostname, parts.path + (("?" + parts.query) if parts.query else "")

    def _timeout(self, deadline: float | None) -> float:
        if deadline is None:
            return self.timeout_seconds
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise WorkerError("PROCESS_TIMEOUT", retryable=True)
        return min(self.timeout_seconds, remaining)

    def _set_socket_timeout(self, connection: http.client.HTTPSConnection, deadline: float | None) -> None:
        timeout = self._timeout(deadline)
        sock = getattr(connection, "sock", None)
        if sock is not None:
            sock.settimeout(timeout)

    @staticmethod
    def _normalized_network_error(error: BaseException) -> WorkerError:
        if isinstance(error, socket.gaierror):
            return WorkerError("OBJECT_DNS_FAILED", retryable=True)
        if isinstance(error, ssl.SSLError):
            return WorkerError("OBJECT_TLS_FAILED")
        if isinstance(error, (socket.timeout, TimeoutError)):
            return WorkerError("OBJECT_TIMEOUT", retryable=True)
        return WorkerError("OBJECT_NETWORK_FAILED", retryable=True)

    def download(self, url: str, destination: Path, max_bytes: int, *, optional: bool = False, deadline: float | None = None) -> bool:
        host, target = self._parts(url)
        connection = http.client.HTTPSConnection(host, timeout=self._timeout(deadline))
        try:
            connection.request("GET", target, headers={"Accept": "application/octet-stream"})
            self._set_socket_timeout(connection, deadline)
            response = connection.getresponse()
            if optional and response.status == 404:
                self._set_socket_timeout(connection, deadline)
                response.read()
                return False
            if response.status != 200:
                self._set_socket_timeout(connection, deadline)
                response.read(4096)
                raise WorkerError("OBJECT_DOWNLOAD_FAILED", retryable=response.status in {408, 429} or response.status >= 500)
            declared = response.getheader("Content-Length")
            declared_bytes: int | None = None
            if declared is not None:
                try:
                    declared_bytes = int(declared)
                except (TypeError, ValueError) as error:
                    raise WorkerError("OBJECT_LENGTH_INVALID") from error
                if declared_bytes < 0:
                    raise WorkerError("OBJECT_LENGTH_INVALID")
            if declared_bytes is not None and declared_bytes > max_bytes:
                raise WorkerError("OBJECT_TOO_LARGE")
            destination.parent.mkdir(parents=True, exist_ok=True)
            total = 0
            with destination.open("wb") as stream:
                while True:
                    self._set_socket_timeout(connection, deadline)
                    read_once = response.read1 if hasattr(response, "read1") else response.read
                    block = read_once(1024 * 1024)
                    if not block:
                        break
                    total += len(block)
                    if total > max_bytes:
                        raise WorkerError("OBJECT_TOO_LARGE")
                    stream.write(block)
            if declared_bytes is not None and total != declared_bytes:
                raise WorkerError("OBJECT_SHORT_READ", retryable=True)
            return True
        except WorkerError:
            destination.unlink(missing_ok=True)
            raise
        except (socket.gaierror, ssl.SSLError, socket.timeout, TimeoutError, http.client.HTTPException, OSError) as error:
            destination.unlink(missing_ok=True)
            raise self._normalized_network_error(error) from error
        finally:
            connection.close()

    def upload(self, url: str, source: Path, max_bytes: int, content_type: str, *, deadline: float | None = None) -> None:
        size = source.stat().st_size
        if size > max_bytes:
            raise WorkerError("OBJECT_TOO_LARGE")
        host, target = self._parts(url)
        connection = http.client.HTTPSConnection(host, timeout=self._timeout(deadline))
        try:
            with source.open("rb") as stream:
                connection.putrequest("PUT", target)
                connection.putheader("Content-Type", content_type)
                connection.putheader("Content-Length", str(size))
                connection.endheaders()
                while True:
                    self._set_socket_timeout(connection, deadline)
                    block = stream.read(1024 * 1024)
                    if not block:
                        break
                    connection.send(block)
            self._set_socket_timeout(connection, deadline)
            response = connection.getresponse()
            self._set_socket_timeout(connection, deadline)
            response.read(4096)
            if response.status < 200 or response.status >= 300:
                raise WorkerError("OBJECT_UPLOAD_FAILED", retryable=response.status in {408, 429} or response.status >= 500)
        except WorkerError:
            raise
        except (socket.gaierror, ssl.SSLError, socket.timeout, TimeoutError, http.client.HTTPException, OSError) as error:
            raise self._normalized_network_error(error) from error
        finally:
            connection.close()


class FakeObjectTransport:
    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}
        self.uploaded: dict[str, bytes] = {}

    @staticmethod
    def _identity(url: str) -> str:
        parts = urlsplit(url)
        return f"{parts.hostname}{parts.path}"

    def seed(self, url: str, value: bytes) -> None:
        self._objects[self._identity(url)] = value

    def remove(self, url: str) -> None:
        self._objects.pop(self._identity(url), None)

    def download(self, url: str, destination: Path, max_bytes: int, *, optional: bool = False, deadline: float | None = None) -> bool:
        if deadline is not None and time.monotonic() >= deadline:
            raise WorkerError("PROCESS_TIMEOUT", retryable=True)
        value = self._objects.get(self._identity(url))
        if value is None:
            if optional:
                return False
            raise WorkerError("OBJECT_DOWNLOAD_FAILED", retryable=True)
        if len(value) > max_bytes:
            raise WorkerError("OBJECT_TOO_LARGE")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(value)
        return True

    def upload(self, url: str, source: Path, max_bytes: int, content_type: str, *, deadline: float | None = None) -> None:
        if deadline is not None and time.monotonic() >= deadline:
            raise WorkerError("PROCESS_TIMEOUT", retryable=True)
        value = source.read_bytes()
        if len(value) > max_bytes:
            raise WorkerError("OBJECT_TOO_LARGE")
        self._objects[self._identity(url)] = value
        self.uploaded[url] = value


class PipelineRunner(Protocol):
    def run(self, sources: list[Path], output_dir: Path, validated: dict[str, Any], deadline: float) -> dict[str, Any]: ...


class CorePipelineRunner:
    def __init__(self, config: WorkerConfig, script: Path | None = None) -> None:
        self.config = config
        self.script = script or Path(__file__).with_name("multibrand_hdr.py")

    @staticmethod
    def _limits(
        memory_bytes: int,
        cpu_seconds: int,
        output_bytes: int,
        open_files: int,
        processes: int,
    ) -> Callable[[], None]:
        def apply() -> None:
            resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
            resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds + 5))
            resource.setrlimit(resource.RLIMIT_FSIZE, (output_bytes, output_bytes))
            resource.setrlimit(resource.RLIMIT_NOFILE, (open_files, open_files))
            if hasattr(resource, "RLIMIT_NPROC"):
                resource.setrlimit(resource.RLIMIT_NPROC, (processes, processes))
            os.setsid()

        return apply

    def run(self, sources: list[Path], output_dir: Path, validated: dict[str, Any], deadline: float) -> dict[str, Any]:
        limits = validated["limits"]
        remaining = max(1.0, deadline - time.monotonic())
        command = [
            sys.executable,
            str(self.script),
            *map(str, sources),
            "--output-dir",
            str(output_dir),
            "--workers",
            str(limits["cpuCores"]),
            "--max-pixels",
            str(limits["maxPixels"]),
            "--max-decompressed-bytes",
            str(limits["maxDecompressedBytes"]),
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=self._limits(
                limits["memoryBytes"],
                max(1, limits["executionMs"] // 1000),
                limits["scratchBytes"],
                self.config.max_open_files,
                self.config.max_processes,
            ),
        )
        interrupted = False
        old_handlers: dict[int, Any] = {}

        def forward_stop(signum: int, _frame: Any) -> None:
            nonlocal interrupted
            interrupted = True
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                old_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, forward_stop)
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._terminate(process)
                    raise WorkerError("PROCESS_TIMEOUT", retryable=True)
                try:
                    stdout, _stderr = process.communicate(timeout=min(1.0, remaining))
                    break
                except subprocess.TimeoutExpired:
                    used = sum(path.stat().st_size for path in output_dir.parent.rglob("*") if path.is_file())
                    if used > limits["scratchBytes"]:
                        self._terminate(process)
                        raise WorkerError("SCRATCH_LIMIT")
        finally:
            for signum, old_handler in old_handlers.items():
                signal.signal(signum, old_handler)
        if interrupted:
            raise WorkerError("CANCELLED")
        if process.returncode == 75:
            raise WorkerError("PROCESS_CHECKPOINTED", retryable=True)
        if process.returncode != 0:
            raise WorkerError("PROCESS_FAILED", retryable=process.returncode < 0)
        try:
            return json.loads(stdout)
        except (json.JSONDecodeError, TypeError) as error:
            raise WorkerError("PROCESS_RESULT_INVALID") from error

    @staticmethod
    def _terminate(process: subprocess.Popen[str]) -> None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()


class HdrWorker:
    def __init__(
        self,
        config: WorkerConfig,
        *,
        transport: ObjectTransport | None = None,
        runner: PipelineRunner | None = None,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        self.config = config
        self.transport = transport or HttpsObjectTransport()
        self.runner = runner or CorePipelineRunner(config)
        self.now_ms = now_ms or (lambda: int(time.time() * 1000))
        self.last_checkpoint_source: str | None = None

    def process(self, dispatch: dict[str, Any]) -> dict[str, Any]:
        self.config.validate()
        self._verify_dispatch(dispatch)
        started = time.monotonic()
        deadline = started + self.config.max_execution_ms / 1000.0
        scratch = Path(tempfile.mkdtemp(prefix=f"{dispatch['jobId']}-", dir=self._work_root()))
        manifest: dict[str, Any] | None = None
        validated: dict[str, Any] | None = None
        try:
            manifest_path = scratch / "manifest.json"
            self.transport.download(dispatch["manifestUrl"], manifest_path, 1024 * 1024, deadline=deadline)
            if file_sha256(manifest_path) != dispatch["manifestSha256"]:
                raise WorkerError("MANIFEST_INTEGRITY")
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise WorkerError("MANIFEST_INVALID") from error
            validated = self._validate_manifest(dispatch, manifest)
            deadline = min(deadline, started + validated["limits"]["executionMs"] / 1000.0)
            self._enforce_scratch_limit(scratch, validated["limits"]["scratchBytes"])

            replay = self._read_completion(validated, scratch, deadline)
            if replay is not None:
                unsigned_replay = {key: value for key, value in replay.items() if key != "evidenceSignature"}
                unsigned_replay["idempotentReplay"] = True
                return self._sign_record(unsigned_replay)

            sources = self._download_inputs(validated, scratch, deadline)
            self._enforce_scratch_limit(scratch, validated["limits"]["scratchBytes"])
            output_dir = scratch / "output"
            output_dir.mkdir()
            self._restore_checkpoint(validated, output_dir / "checkpoints", scratch, deadline)
            self._enforce_scratch_limit(scratch, validated["limits"]["scratchBytes"])
            try:
                execution_seconds = validated["limits"]["executionMs"] / 1000.0
                reserve = min(120.0, max(1.0, execution_seconds * 0.1))
                runner_deadline = deadline - reserve
                if runner_deadline <= time.monotonic():
                    raise WorkerError("PROCESS_TIMEOUT", retryable=True)
                report = self.runner.run(sources, output_dir, validated, runner_deadline)
            except WorkerError:
                # The core catches normal termination and atomically updates its
                # local merge state. Persist that state before this invocation is
                # reported retryable and the ephemeral volume is removed.
                self._save_checkpoint(validated, output_dir / "checkpoints", scratch, deadline)
                raise
            self._save_checkpoint(validated, output_dir / "checkpoints", scratch, deadline)
            self._enforce_scratch_limit(scratch, validated["limits"]["scratchBytes"])
            evidence = self._publish_result(validated, report, sources, scratch, deadline)
            self._write_json(validated["completion"]["uploadUrl"], evidence, scratch / "completion.json", deadline)
            return evidence
        except WorkerError as error:
            if validated is not None:
                self._archive_failure_best_effort(validated, error, scratch, deadline)
            raise
        finally:
            shutil.rmtree(scratch, ignore_errors=True)

    def _work_root(self) -> Path:
        self.config.work_root.mkdir(parents=True, exist_ok=True)
        return self.config.work_root

    @staticmethod
    def _enforce_scratch_limit(scratch: Path, limit: int) -> None:
        try:
            used = sum(path.stat().st_size for path in scratch.rglob("*") if path.is_file())
        except OSError as error:
            raise WorkerError("SCRATCH_MEASURE_FAILED", retryable=True) from error
        if used > limit:
            raise WorkerError("SCRATCH_LIMIT")

    def _verify_dispatch(self, dispatch: dict[str, Any]) -> None:
        required = {
            "version", "audience", "jobId", "tenantId", "projectId", "groupId",
            "manifestObjectKey", "manifestUrl", "manifestSha256", "expiresAt", "signature",
        }
        if set(dispatch) != required or dispatch.get("version") != "hdr-dispatch.v1" or dispatch.get("audience") != "runpod-hdr":
            raise WorkerError("AUTH_INVALID")
        if not all(IDENTIFIER.fullmatch(str(dispatch.get(field, ""))) for field in ("jobId", "tenantId", "projectId", "groupId")):
            raise WorkerError("AUTH_INVALID")
        if not SHA256.fullmatch(str(dispatch.get("manifestSha256", ""))):
            raise WorkerError("AUTH_INVALID")
        signature = str(dispatch.get("signature", ""))
        expected = hmac.new(self.config.manifest_secret, dispatch_signature_payload(dispatch), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise WorkerError("AUTH_INVALID")
        if not isinstance(dispatch["expiresAt"], int) or self.now_ms() >= dispatch["expiresAt"]:
            raise WorkerError("AUTH_EXPIRED")
        job_prefix = f"users/{dispatch['tenantId']}/projects/{dispatch['projectId']}/jobs/{dispatch['jobId']}/"
        manifest_name = dispatch["manifestObjectKey"][len(job_prefix):] if dispatch["manifestObjectKey"].startswith(job_prefix) else ""
        if not re.fullmatch(r"manifest-[1-9][0-9]*\.json", manifest_name):
            raise WorkerError("IDENTITY_MISMATCH")
        self._validate_object_url(dispatch["manifestUrl"], dispatch["manifestObjectKey"])

    def _validate_manifest(self, dispatch: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
        if manifest.get("version") != "hdr-worker-manifest.v1" or not isinstance(manifest.get("job"), dict):
            raise WorkerError("MANIFEST_INVALID")
        job = manifest["job"]
        for field in ("jobId", "tenantId", "projectId", "groupId"):
            if job.get(field) != dispatch[field]:
                raise WorkerError("IDENTITY_MISMATCH")
        if not isinstance(job.get("attempt"), int) or job["attempt"] < 1:
            raise WorkerError("MANIFEST_INVALID")
        expected_manifest_key = (
            f"users/{dispatch['tenantId']}/projects/{dispatch['projectId']}/jobs/"
            f"{dispatch['jobId']}/manifest-{job['attempt']}.json"
        )
        if dispatch["manifestObjectKey"] != expected_manifest_key:
            raise WorkerError("IDENTITY_MISMATCH")
        if not isinstance(manifest.get("expiresAt"), int) or self.now_ms() >= manifest["expiresAt"]:
            raise WorkerError("MANIFEST_EXPIRED")
        limits = manifest.get("limits")
        ceilings = {
            "cpuCores": self.config.max_cpu_cores,
            "memoryBytes": self.config.max_memory_bytes,
            "scratchBytes": self.config.max_scratch_bytes,
            "maxInputBytes": self.config.max_input_bytes,
            "maxOutputBytes": self.config.max_output_bytes,
            "maxPixels": self.config.max_pixels,
            "maxDecompressedBytes": self.config.max_decompressed_bytes,
            "executionMs": self.config.max_execution_ms,
            "maxAttempts": self.config.max_attempts,
        }
        if not isinstance(limits, dict):
            raise WorkerError("LIMIT_INVALID")
        for name, ceiling in ceilings.items():
            value = limits.get(name)
            if not isinstance(value, int) or value <= 0 or value > ceiling:
                raise WorkerError("LIMIT_INVALID")
        if job["attempt"] > limits["maxAttempts"]:
            raise WorkerError("ATTEMPT_EXHAUSTED")

        inputs = manifest.get("inputs")
        if not isinstance(inputs, list) or len(inputs) < 2 or len(inputs) > 20:
            raise WorkerError("INPUT_INVALID")
        total = 0
        dimensions: set[tuple[int, int]] = set()
        exposures: set[float] = set()
        names: set[str] = set()
        object_identities: set[tuple[str, str]] = set()
        input_prefix = f"users/{dispatch['tenantId']}/projects/{dispatch['projectId']}/originals/"
        for item in inputs:
            if not isinstance(item, dict) or set(("objectKey", "versionId", "filename", "bytes", "sha256", "width", "height", "exposureBias", "downloadUrl", "urlExpiresAt")) - set(item):
                raise WorkerError("INPUT_INVALID")
            if Path(str(item["filename"])).name != item["filename"] or Path(item["filename"]).suffix.lower() not in SUPPORTED_INPUTS:
                raise WorkerError("INPUT_INVALID")
            if item["filename"] in names:
                raise WorkerError("INPUT_INVALID")
            names.add(item["filename"])
            if not IDENTIFIER.fullmatch(str(item["versionId"])):
                raise WorkerError("INPUT_INVALID")
            object_identity = (str(item["objectKey"]), str(item["versionId"]))
            if object_identity in object_identities:
                raise WorkerError("INPUT_INVALID")
            object_identities.add(object_identity)
            if not isinstance(item["bytes"], int) or item["bytes"] <= 0 or not SHA256.fullmatch(str(item["sha256"])):
                raise WorkerError("INPUT_INVALID")
            if not isinstance(item["width"], int) or not isinstance(item["height"], int) or item["width"] <= 0 or item["height"] <= 0:
                raise WorkerError("INPUT_INVALID")
            if item["width"] * item["height"] > limits["maxPixels"]:
                raise WorkerError("PIXEL_LIMIT")
            if isinstance(item["exposureBias"], bool) or not isinstance(item["exposureBias"], (int, float)) or not math.isfinite(item["exposureBias"]):
                raise WorkerError("EXPOSURE_INVALID")
            dimensions.add((item["width"], item["height"]))
            exposures.add(round(float(item["exposureBias"]), 6))
            total += item["bytes"]
            self._validate_key_prefix(item["objectKey"], input_prefix)
            self._validate_object_url(item["downloadUrl"], item["objectKey"])
            self._validate_url_expiry(item, manifest["expiresAt"])
        if len(exposures) < 2:
            raise WorkerError("EXPOSURE_INVALID")
        if len(dimensions) != 1:
            raise WorkerError("DIMENSION_MISMATCH")
        if total > limits["maxInputBytes"] or total + limits["maxOutputBytes"] > limits["scratchBytes"]:
            raise WorkerError("SCRATCH_LIMIT")
        if shutil.disk_usage(self._work_root()).free < limits["scratchBytes"]:
            raise WorkerError("SCRATCH_CAPACITY", retryable=True)

        base = f"users/{dispatch['tenantId']}/projects/{dispatch['projectId']}"
        job_base = f"{base}/jobs/{dispatch['jobId']}"
        expected_keys = {
            ("output", "objectKey"): f"{base}/intermediates/{dispatch['groupId']}-hdr.jpg",
            ("output", "metadataObjectKey"): f"{base}/intermediates/{dispatch['groupId']}-hdr.evidence.json",
            ("checkpoint", "currentObjectKey"): f"{job_base}/checkpoints/current.tar.gz",
            ("checkpoint", "previousObjectKey"): f"{job_base}/checkpoints/previous.tar.gz",
            ("completion", "objectKey"): f"{job_base}/completion.json",
            ("failure", "objectKey"): f"{job_base}/failures/{job['attempt']}.json",
        }
        for section, pairs in {
            "output": (("objectKey", "downloadUrl"), ("objectKey", "uploadUrl"), ("metadataObjectKey", "metadataDownloadUrl"), ("metadataObjectKey", "metadataUploadUrl")),
            "checkpoint": (("currentObjectKey", "currentDownloadUrl"), ("currentObjectKey", "currentUploadUrl"), ("previousObjectKey", "previousDownloadUrl"), ("previousObjectKey", "previousUploadUrl")),
            "completion": (("objectKey", "downloadUrl"), ("objectKey", "uploadUrl")),
            "failure": (("objectKey", "uploadUrl"),),
        }.items():
            value = manifest.get(section)
            if not isinstance(value, dict):
                raise WorkerError("MANIFEST_INVALID")
            for key_field, url_field in pairs:
                if not isinstance(value.get(key_field), str) or not isinstance(value.get(url_field), str):
                    raise WorkerError("MANIFEST_INVALID")
                if value[key_field] != expected_keys[(section, key_field)]:
                    raise WorkerError("IDENTITY_MISMATCH")
                self._validate_object_url(value[url_field], value[key_field])
            self._validate_url_expiry(value, manifest["expiresAt"])
        output = manifest["output"]
        expected_width, expected_height = next(iter(dimensions))
        if output.get("format") != "jpeg" or output.get("preserveInputDimensions") is not True or output.get("width") != expected_width or output.get("height") != expected_height:
            raise WorkerError("OUTPUT_CONTRACT_INVALID")
        return manifest

    def _validate_url_expiry(self, value: dict[str, Any], manifest_expiry: int) -> None:
        expiry = value.get("urlExpiresAt")
        if not isinstance(expiry, int) or expiry < self.now_ms() or expiry > manifest_expiry:
            raise WorkerError("URL_EXPIRED")

    def _validate_key_prefix(self, key: str, prefix: str) -> None:
        if not isinstance(key, str) or not key.startswith(prefix) or ".." in Path(key).parts or key.startswith("/"):
            raise WorkerError("IDENTITY_MISMATCH")

    def _validate_object_url(self, url: str, object_key: str) -> None:
        try:
            parts = urlsplit(url)
            port = parts.port
        except ValueError as error:
            raise WorkerError("URL_NOT_ALLOWED") from error
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.hostname.lower() not in self.config.allowed_hosts
            or port not in (None, 443)
            or parts.username is not None
            or parts.password is not None
            or parts.fragment
            or unquote(parts.path) != self.config.object_path_prefix + object_key
        ):
            raise WorkerError("URL_NOT_ALLOWED")

    def _download_inputs(self, manifest: dict[str, Any], scratch: Path, deadline: float) -> list[Path]:
        sources: list[Path] = []
        inputs_dir = scratch / "inputs"
        for index, item in enumerate(manifest["inputs"]):
            if time.monotonic() >= deadline:
                raise WorkerError("PROCESS_TIMEOUT", retryable=True)
            destination = inputs_dir / f"{index:02d}-{item['filename']}"
            self.transport.download(item["downloadUrl"], destination, item["bytes"], deadline=deadline)
            if destination.stat().st_size != item["bytes"] or file_sha256(destination) != item["sha256"]:
                raise WorkerError("INPUT_INTEGRITY")
            sources.append(destination)
        return sources

    def _read_completion(self, manifest: dict[str, Any], scratch: Path, deadline: float) -> dict[str, Any] | None:
        path = scratch / "existing-completion.json"
        recovered_from_output_evidence = False
        if not self.transport.download(manifest["completion"]["downloadUrl"], path, 1024 * 1024, optional=True, deadline=deadline):
            path = scratch / "existing-output-evidence.json"
            if not self.transport.download(manifest["output"]["metadataDownloadUrl"], path, 1024 * 1024, optional=True, deadline=deadline):
                return None
            recovered_from_output_evidence = True
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise WorkerError("COMPLETION_INVALID") from error
        identity = manifest["job"]
        if not self._verify_signed_record(value):
            raise WorkerError("COMPLETION_INVALID")
        if (value.get("version") != "hdr-completion.v1" or value.get("status") != "completed"
                or any(value.get(field) != identity[field] for field in ("jobId", "tenantId", "projectId", "groupId"))
                or isinstance(value.get("attempt"), bool) or not isinstance(value.get("attempt"), int)
                or value["attempt"] != identity["attempt"]):
            raise WorkerError("COMPLETION_INVALID")
        output = value.get("output", {})
        expected_output = manifest["output"]
        if (not SHA256.fullmatch(str(output.get("sha256", "")))
                or output.get("objectKey") != expected_output["objectKey"]
                or output.get("format") != "jpeg"
                or output.get("width") != expected_output["width"]
                or output.get("height") != expected_output["height"]):
            raise WorkerError("COMPLETION_INVALID")
        if not isinstance(output.get("bytes"), int) or output["bytes"] <= 0 or output["bytes"] > manifest["limits"]["maxOutputBytes"]:
            raise WorkerError("COMPLETION_INVALID")
        expected_inputs = [
            {field: item[field] for field in ("objectKey", "versionId", "sha256", "bytes")}
            for item in manifest["inputs"]
        ]
        if value.get("inputs") != expected_inputs:
            raise WorkerError("COMPLETION_INVALID")
        existing_output = scratch / "existing-output.jpg"
        if not self.transport.download(manifest["output"]["downloadUrl"], existing_output, output["bytes"], optional=True, deadline=deadline):
            raise WorkerError("COMPLETION_OUTPUT_MISSING", retryable=True)
        if existing_output.stat().st_size != output["bytes"] or file_sha256(existing_output) != output["sha256"]:
            raise WorkerError("COMPLETION_OUTPUT_MISMATCH")
        try:
            with Image.open(existing_output) as image:
                if image.format != "JPEG" or image.size != (output.get("width"), output.get("height")):
                    raise WorkerError("COMPLETION_OUTPUT_MISMATCH")
        except OSError as error:
            raise WorkerError("COMPLETION_OUTPUT_MISMATCH") from error
        if recovered_from_output_evidence:
            self._write_json(manifest["completion"]["uploadUrl"], value, scratch / "repaired-completion.json", deadline)
        return value

    def _checkpoint_metadata(self, manifest: dict[str, Any], files: list[dict[str, Any]]) -> dict[str, Any]:
        value = {
            "version": "hdr-checkpoint-archive.v1",
            **{field: manifest["job"][field] for field in ("jobId", "tenantId", "projectId", "groupId")},
            "inputIdentitySha256": self._input_identity_sha256(manifest),
            "files": files,
        }
        value["signature"] = hmac.new(self.config.manifest_secret, canonical_json(value), hashlib.sha256).hexdigest()
        return value

    @staticmethod
    def _input_identity_sha256(manifest: dict[str, Any]) -> str:
        identity = [
            {field: item[field] for field in ("objectKey", "versionId", "sha256", "bytes")}
            for item in manifest["inputs"]
        ]
        return hashlib.sha256(canonical_json(identity)).hexdigest()

    def _create_checkpoint_archive(self, checkpoint_dir: Path, archive: Path, manifest: dict[str, Any]) -> bool:
        if not checkpoint_dir.is_dir():
            return False
        files = []
        for path in sorted(checkpoint_dir.rglob("*")):
            if path.is_symlink():
                raise WorkerError("CHECKPOINT_INVALID")
            if path.is_file():
                relative = path.relative_to(checkpoint_dir).as_posix()
                files.append({"path": relative, "bytes": path.stat().st_size, "sha256": file_sha256(path)})
        if not files:
            return False
        metadata = self._checkpoint_metadata(manifest, files)
        with tarfile.open(archive, "w:gz") as package:
            package.add(checkpoint_dir, arcname="checkpoint", recursive=True)
            encoded = canonical_json(metadata)
            info = tarfile.TarInfo("checkpoint-metadata.json")
            info.size = len(encoded)
            info.mode = 0o600
            package.addfile(info, io.BytesIO(encoded))
        if archive.stat().st_size > self.config.checkpoint_archive_bytes:
            raise WorkerError("CHECKPOINT_TOO_LARGE")
        return True

    def _extract_checkpoint_archive(self, archive: Path, destination: Path, manifest: dict[str, Any]) -> bool:
        try:
            with tarfile.open(archive, "r:gz") as package:
                members = package.getmembers()
                if len(members) > 10000 or any(member.issym() or member.islnk() or member.isdev() for member in members):
                    return False
                total = sum(member.size for member in members if member.isfile())
                if total > self.config.checkpoint_archive_bytes:
                    return False
                for member in members:
                    target = (destination.parent / member.name).resolve()
                    if not target.is_relative_to(destination.parent.resolve()):
                        return False
                metadata_member = package.getmember("checkpoint-metadata.json")
                stream = package.extractfile(metadata_member)
                if stream is None:
                    return False
                metadata = json.loads(stream.read().decode("utf-8"))
                signature = metadata.pop("signature", "")
                expected = hmac.new(self.config.manifest_secret, canonical_json(metadata), hashlib.sha256).hexdigest()
                if not hmac.compare_digest(str(signature), expected):
                    return False
                if any(metadata.get(field) != manifest["job"][field] for field in ("jobId", "tenantId", "projectId", "groupId")):
                    return False
                if metadata.get("inputIdentitySha256") != self._input_identity_sha256(manifest):
                    return False
                file_records = metadata.get("files")
                if not isinstance(file_records, list) or any(not isinstance(item, dict) for item in file_records):
                    return False
                expected_members = {f"checkpoint/{item.get('path')}" for item in file_records}
                archive_files = {member.name for member in members if member.isfile() and member.name != "checkpoint-metadata.json"}
                if archive_files != expected_members:
                    return False
                extraction = destination.parent / "checkpoint-restore"
                shutil.rmtree(extraction, ignore_errors=True)
                extraction.mkdir(parents=True)
                for member in members:
                    if not member.isfile() or member.name == "checkpoint-metadata.json":
                        continue
                    if member.name not in expected_members:
                        return False
                    stream = package.extractfile(member)
                    if stream is None:
                        return False
                    target = extraction / member.name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with target.open("wb") as output:
                        shutil.copyfileobj(stream, output, length=1024 * 1024)
            restored = extraction / "checkpoint"
            for item in file_records:
                path = restored / item["path"]
                if not path.is_file() or path.stat().st_size != item["bytes"] or file_sha256(path) != item["sha256"]:
                    shutil.rmtree(extraction, ignore_errors=True)
                    return False
            shutil.rmtree(destination, ignore_errors=True)
            os.replace(restored, destination)
            shutil.rmtree(extraction, ignore_errors=True)
            return True
        except (tarfile.TarError, KeyError, OSError, ValueError, json.JSONDecodeError, TypeError):
            return False

    def _restore_checkpoint(self, manifest: dict[str, Any], destination: Path, scratch: Path, deadline: float) -> None:
        self.last_checkpoint_source = None
        for name in ("current", "previous"):
            archive = scratch / f"checkpoint-{name}.tar.gz"
            url = manifest["checkpoint"][f"{name}DownloadUrl"]
            if self.transport.download(url, archive, self.config.checkpoint_archive_bytes, optional=True, deadline=deadline) and self._extract_checkpoint_archive(archive, destination, manifest):
                self.last_checkpoint_source = name
                return

    def _save_checkpoint(self, manifest: dict[str, Any], checkpoint_dir: Path, scratch: Path, deadline: float) -> None:
        archive = scratch / "checkpoint-new.tar.gz"
        if not self._create_checkpoint_archive(checkpoint_dir, archive, manifest):
            return
        current = scratch / "checkpoint-old.tar.gz"
        if self.transport.download(manifest["checkpoint"]["currentDownloadUrl"], current, self.config.checkpoint_archive_bytes, optional=True, deadline=deadline):
            verification = scratch / "old-verification"
            if self._extract_checkpoint_archive(current, verification, manifest):
                self.transport.upload(manifest["checkpoint"]["previousUploadUrl"], current, self.config.checkpoint_archive_bytes, "application/gzip", deadline=deadline)
        self.transport.upload(manifest["checkpoint"]["currentUploadUrl"], archive, self.config.checkpoint_archive_bytes, "application/gzip", deadline=deadline)

    def _publish_result(self, manifest: dict[str, Any], report: dict[str, Any], sources: list[Path], scratch: Path, deadline: float) -> dict[str, Any]:
        if report.get("status") != "complete" or not report.get("quality_gates", {}).get("output_dimensions_preserved"):
            raise WorkerError("QUALITY_GATE_FAILED")
        raw_path = report.get("render", {}).get("jpeg")
        if not isinstance(raw_path, str):
            raise WorkerError("OUTPUT_INVALID")
        output_path = Path(raw_path).resolve()
        if not output_path.is_file() or not output_path.is_relative_to(scratch.resolve()):
            raise WorkerError("OUTPUT_INVALID")
        if output_path.stat().st_size > manifest["limits"]["maxOutputBytes"]:
            raise WorkerError("OUTPUT_TOO_LARGE")
        try:
            with Image.open(output_path) as image:
                image.verify()
            with Image.open(output_path) as image:
                width, height = image.size
                image_format = image.format
        except (OSError, ValueError) as error:
            raise WorkerError("OUTPUT_INVALID") from error
        expected = manifest["output"]
        if image_format != "JPEG" or width != expected["width"] or height != expected["height"]:
            raise WorkerError("OUTPUT_DIMENSION_MISMATCH")
        digest = file_sha256(output_path)
        evidence = {
            "version": "hdr-completion.v1",
            "status": "completed",
            **{field: manifest["job"][field] for field in ("jobId", "tenantId", "projectId", "groupId")},
            "attempt": manifest["job"]["attempt"],
            "completedAt": self.now_ms(),
            "output": {
                "objectKey": expected["objectKey"],
                "sha256": digest,
                "bytes": output_path.stat().st_size,
                "width": width,
                "height": height,
                "format": "jpeg",
            },
            "inputs": [
                {"objectKey": item["objectKey"], "versionId": item["versionId"], "sha256": file_sha256(path), "bytes": path.stat().st_size}
                for item, path in zip(manifest["inputs"], sources)
            ],
            "idempotentReplay": False,
        }
        evidence = self._sign_record(evidence)
        self.transport.upload(expected["uploadUrl"], output_path, manifest["limits"]["maxOutputBytes"], "image/jpeg", deadline=deadline)
        self._write_json(expected["metadataUploadUrl"], evidence, scratch / "output-evidence.json", deadline)
        return evidence

    def _write_json(self, url: str, value: dict[str, Any], path: Path, deadline: float) -> None:
        path.write_bytes(canonical_json(value))
        self.transport.upload(url, path, 1024 * 1024, "application/json", deadline=deadline)

    def _archive_failure_best_effort(self, manifest: dict[str, Any], error: WorkerError, scratch: Path, deadline: float) -> None:
        try:
            job = manifest.get("job", {})
            limits = manifest.get("limits", {})
            attempt = job.get("attempt", 1)
            max_attempts = limits.get("maxAttempts", 1)
            value = {
                "version": "hdr-failure.v1",
                "status": "cancelled" if error.code == "CANCELLED" else ("dead_letter" if attempt >= max_attempts or not error.retryable else "retryable"),
                "code": error.code,
                "retryable": error.retryable and attempt < max_attempts,
                "attempt": attempt,
                **{field: job.get(field) for field in ("jobId", "tenantId", "projectId", "groupId")},
                "recordedAt": self.now_ms(),
            }
            value = self._sign_record(value)
            failure = manifest.get("failure", {})
            if isinstance(failure.get("uploadUrl"), str):
                self._write_json(failure["uploadUrl"], value, scratch / "failure.json", deadline)
        except Exception:
            # The platform invocation still fails and is retried/DLQed upstream;
            # failure telemetry must never mask the primary failure.
            pass

    def _sign_record(self, value: dict[str, Any]) -> dict[str, Any]:
        signed = dict(value)
        signed["evidenceSignature"] = hmac.new(self.config.manifest_secret, canonical_json(value), hashlib.sha256).hexdigest()
        return signed

    def _verify_signed_record(self, value: dict[str, Any]) -> bool:
        if not isinstance(value, dict):
            return False
        unsigned = dict(value)
        signature = unsigned.pop("evidenceSignature", "")
        expected = hmac.new(self.config.manifest_secret, canonical_json(unsigned), hashlib.sha256).hexdigest()
        return hmac.compare_digest(str(signature), expected)
