"""Small synthetic benchmark; never reads customer files or proves RAW capacity."""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import shutil
import time

import numpy as np

from multibrand_hdr import merge_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--width", type=int, default=2048)
    parser.add_argument("--rows", type=int, default=128)
    parser.add_argument("--exposures", type=int, choices=range(2, 10), default=5)
    args = parser.parse_args()
    if args.width <= 0 or args.rows <= 0 or args.width * args.rows > 2_000_000:
        raise SystemExit("fixture is limited to 2,000,000 pixels")
    memory_bytes = args.exposures * args.rows * args.width * 3 * 4
    if memory_bytes > 512 * 1024 * 1024:
        raise SystemExit("fixture allocation exceeds 512 MiB")

    rng = np.random.default_rng(20260728)
    sensors = rng.uniform(0.002, 0.85, size=(args.exposures, args.rows, args.width, 3)).astype(np.float32)
    scales = np.geomspace(1.0, 16.0, args.exposures).astype(np.float32)
    valid = np.ones((args.exposures, args.rows, args.width), dtype=bool)
    started = time.perf_counter()
    result = merge_rows(sensors, scales, valid, shortest_index=0)
    elapsed = time.perf_counter() - started
    disk = shutil.disk_usage("/tmp")
    report = {
        "kind": "synthetic-row-merge-only",
        "qualifiedCapacityResult": False,
        "hardware": {
            "platform": platform.platform(),
            "logicalCpu": os.cpu_count(),
            "tmpFreeBytes": disk.free,
        },
        "fixture": {"width": args.width, "rows": args.rows, "exposures": args.exposures},
        "measured": {
            "elapsedSeconds": elapsed,
            "megapixelsPerSecond": (args.width * args.rows / 1_000_000) / elapsed,
            "maxRssPlatformUnits": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "resultShape": list(result.shape),
        },
        "warning": "This does not decode RAW, align images, render full resolution, test R2, or qualify concurrency.",
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

