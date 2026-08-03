"""Plain HTTP entrypoint for RunPod Load-balancer (custom API) endpoints.

Receives {"input": <signed hdr-dispatch.v1>} on POST /run and returns the
worker result directly. The RunPod queue/SDK protocol is not involved; the
load balancer routes HTTP requests straight to this container.

RunPod injects PORT (business HTTP server) and PORT_HEALTH (health-check
server, must differ from PORT). The health server answers /ping.
"""

from __future__ import annotations

import os
import threading
import traceback
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from serverless_worker import HdrWorker, WorkerConfig, WorkerError

app = FastAPI(title="jin-lumeo-hdr", docs_url=None, openapi_url=None)

PORT = int(os.environ.get("PORT", "8000") or "8000")
PORT_HEALTH = int(os.environ.get("PORT_HEALTH", "8001") or "8001")

_worker: HdrWorker | None = None


def configured_worker() -> HdrWorker:
    global _worker
    if _worker is None:
        _worker = HdrWorker(WorkerConfig.from_environment())
    return _worker


def handle_event(event: dict[str, Any], worker: HdrWorker | None = None) -> dict[str, Any]:
    if not isinstance(event, dict) or not isinstance(event.get("input"), dict):
        raise RuntimeError("REQUEST_INVALID")
    try:
        return (worker or configured_worker()).process(event["input"])
    except WorkerError as error:
        # Only the stable code reaches provider logs. Presigned URLs, object keys,
        # customer filenames, secrets and stack details remain out of the error.
        raise RuntimeError(error.code) from None


class PingHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 (http.server naming)
        body = b'{"status":"ok"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return


def start_health_server() -> None:
    server = HTTPServer(("0.0.0.0", PORT_HEALTH), PingHandler)
    server.serve_forever()


@app.get("/ping")
async def ping() -> dict[str, str]:
    return {"status": "ok"}


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


def handler(event: dict[str, Any]) -> dict[str, Any]:
    """RunPod SDK queue-mode handler; the platform delivers {"input": dispatch}."""
    return handle_event(event)


if __name__ == "__main__":
    if os.environ.get("RUNPOD_WEBHOOK_GET_JOB"):
        # Queue-based serverless: let the RunPod SDK poll jobs for us.
        import runpod

        print("starting runpod SDK queue worker", flush=True)
        runpod.serverless.start({"handler": handler})
    else:
        # Load-balancer mode: serve HTTP directly.
        import uvicorn

        threading.Thread(target=start_health_server, daemon=True).start()
        print(f"starting business server on {PORT}, health server on {PORT_HEALTH}", flush=True)
        uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
