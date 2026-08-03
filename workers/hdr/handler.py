"""RunPod Serverless entrypoint for the restricted HDR worker."""

from __future__ import annotations

from typing import Any

from serverless_worker import HdrWorker, WorkerConfig, WorkerError


_worker: HdrWorker | None = None


def configured_worker() -> HdrWorker:
    global _worker
    if _worker is None:
        _worker = HdrWorker(WorkerConfig.from_environment())
    return _worker


def handle_event(event: dict[str, Any], worker: HdrWorker | None = None) -> dict[str, Any]:
    if not isinstance(event, dict) or set(event) - {"id", "input", "webhook", "policy"} or not isinstance(event.get("input"), dict):
        raise RuntimeError("REQUEST_INVALID")
    try:
        return (worker or configured_worker()).process(event["input"])
    except WorkerError as error:
        # Only the stable code reaches provider logs. Presigned URLs, object keys,
        # customer filenames, secrets and stack details remain out of the error.
        raise RuntimeError(error.code) from None


def handler(event: dict[str, Any]) -> dict[str, Any]:
    return handle_event(event)


if __name__ == "__main__":
    import runpod

    runpod.serverless.start({"handler": handler})

