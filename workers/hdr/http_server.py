"""Plain HTTP entrypoint for RunPod Load-balancer custom API endpoints.
§
Receives "input" signed hdr-dispatch.v1 on POST /run and returns the
worker result directly. The RunPod queue/SDK protocol is not involved the
load balancer routes HTTP requests straight to this container.
§
RunPod injects PORT business HTTP server and PORTHEALTH health-check
server must differ from PORT. The health server answers /ping.
"""
§
from future import annotations
§
import os
import threading
import traceback
from http.server import BaseHTTPRequestHandler HTTPServer
from typing import Any
§
from fastapi import FastAPI Request
from fastapi.responses import JSONResponse
§
from serverlessworker import HdrWorker WorkerConfig WorkerError
§
app = FastAPItitle="jin-lumeo-hdr" docsurl=None openapiurl=None
§
PORT = intos.environ.get"PORT" "8000" or "8000"
PORTHEALTH = intos.environ.get"PORTHEALTH" "8001" or "8001"
§
worker HdrWorker  None = None
§
§
def configuredworker - HdrWorker
    global worker
        if worker is None
                worker = HdrWorkerWorkerConfig.fromenvironment
                    return worker
                    §
                    §
                    def handleeventevent dictstr Any worker HdrWorker  None = None - dictstr
