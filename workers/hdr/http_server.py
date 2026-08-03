"""Plain HTTP entrypoint for RunPod Load-balancer (custom API) endpoints.

Receives {"input": <signed hdr-dispatch.v1>} on POST /run and returns the
worker result directly. The RunPod queue/SDK protocol is not involved; the
load balancer routes HTTP requests straight to this container.
"""

from __future__ import annotations

import traceback
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from serverless_worker import HdrWorker, WorkerConfig, WorkerError

app = FastAPI(title="jin-lumeo-hdr", docs_url=None, openapi_url=None)

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


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/run")
async def run(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "REQUEST_INVALID"}, status_code=400)
    if not isinstance(body, dict) or "input" not in body:
        return JSONResponse({"error": "REQUEST_INVALID"}, status_code=400)
    try:
        result = handle_event(body)
        return JSONResponse({"output": result})
    except RuntimeError as error:
        return JSONResponse({"error": str(error)})
    except Exception:
        # Never leak internal details; the traceback goes to container logs.
        traceback.print_exc()
        return JSONResponse({"error": "INTERNAL_ERROR"}, status_code=500)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
