from __future__ import annotations

import unittest
import json
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class ContainerContractTests(unittest.TestCase):
    def test_image_is_digest_pinned_non_root_fail_closed_and_narrow(self) -> None:
        dockerfile = (ROOT / "workers/hdr/Dockerfile").read_text(encoding="utf-8")
        self.assertIn("@sha256:*", dockerfile)
        self.assertIn("HDR_WORKER_ENABLED=false", dockerfile)
        self.assertIn("USER hdrworker", dockerfile)
        self.assertNotIn("COPY . ", dockerfile)
        self.assertIn("workers/hdr/multibrand_hdr.py workers/hdr/serverless_worker.py workers/hdr/handler.py", dockerfile)

    def test_private_workflow_fixture_is_excluded_from_build_context(self) -> None:
        ignored = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        self.assertTrue(any(line.strip().rstrip("/") == "assets/hdr-workflow" for line in ignored))

    def test_serverless_entrypoint_is_present(self) -> None:
        handler = (ROOT / "workers/hdr/handler.py").read_text(encoding="utf-8")
        self.assertIn('runpod.serverless.start({"handler": handler})', handler)

    def test_supply_chain_preflight_is_machine_readable_and_fail_closed(self) -> None:
        policy = json.loads((ROOT / "workers/hdr/container-supply-chain-policy.json").read_text(encoding="utf-8"))
        self.assertEqual(policy["schemaVersion"], "hdr-container-supply-chain.v1")
        self.assertEqual(policy["approvedBaseImages"], [])
        result = subprocess.run(
            [shutil.which("node") or "node", "scripts/hdr-container-evidence.mjs", "--preflight"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
            env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
        )
        self.assertEqual(result.returncode, 2)
        evidence = json.loads(result.stdout)
        self.assertEqual(evidence["schemaVersion"], "hdr-container-evidence.v1")
        self.assertEqual(evidence["status"], "blocked")
        self.assertIn("approved_base_image_missing", evidence["blockers"])
        self.assertIn("hashed_python_lock_missing", evidence["blockers"])

    def test_build_script_requires_preflight_and_archives_scan_evidence(self) -> None:
        script = (ROOT / "workers/hdr/build-image.sh").read_text(encoding="utf-8")
        self.assertIn("hdr-container-evidence.mjs --build", script)
        evidence_script = (ROOT / "scripts/hdr-container-evidence.mjs").read_text(encoding="utf-8")
        self.assertIn("sbom.spdx.json", evidence_script)
        self.assertIn("vulnerability-report.json", evidence_script)
        self.assertIn("image-inspect.json", evidence_script)


if __name__ == "__main__":
    unittest.main()
