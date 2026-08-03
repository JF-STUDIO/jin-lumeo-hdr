from __future__ import annotations

import unittest

from handler import handle_event
from serverless_worker import WorkerError


class StubWorker:
    def __init__(self, error: WorkerError | None = None) -> None:
        self.error = error
        self.received = None

    def process(self, value: dict) -> dict:
        self.received = value
        if self.error:
            raise self.error
        return {"status": "completed", "jobId": "safe-job"}


class HandlerTests(unittest.TestCase):
    def test_passes_only_input_contract_to_worker(self) -> None:
        worker = StubWorker()
        result = handle_event({"id": "provider-id", "input": {"version": "hdr-dispatch.v1"}}, worker)  # type: ignore[arg-type]
        self.assertEqual(result["status"], "completed")
        self.assertEqual(worker.received, {"version": "hdr-dispatch.v1"})

    def test_rejects_unknown_request_fields_and_sanitizes_worker_failure(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "REQUEST_INVALID"):
            handle_event({"input": {}, "attacker": "value"}, StubWorker())  # type: ignore[arg-type]
        with self.assertRaisesRegex(RuntimeError, "PROCESS_TIMEOUT") as raised:
            handle_event({"input": {}}, StubWorker(WorkerError("PROCESS_TIMEOUT", retryable=True)))  # type: ignore[arg-type]
        self.assertNotIn("http", str(raised.exception).lower())


if __name__ == "__main__":
    unittest.main()

