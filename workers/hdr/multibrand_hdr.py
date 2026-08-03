#!/usr/bin/env python3
"""Deterministic multi-brand RAW bracket HDR merge for real-estate photographs.

The merge mathematics intentionally preserves the previously validated Jin Lumeo
method: measured exposure ratios, SIFT/RANSAC alignment, smooth highlight/shadow
weights, stop-domain deghosting, and a shortest-exposure fallback.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import signal
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import cv2
import numpy as np
import rawpy
import tifffile
from PIL import Image, ImageCms


SUPPORTED_RAW_EXTENSIONS = {
    ".cr2": "Canon",
    ".cr3": "Canon",
    ".nef": "Nikon",
    ".nrw": "Nikon",
    ".arw": "Sony",
    ".raf": "Fujifilm",
}

XYZ_TO_SRGB = np.array(
    [
        [3.24096994, -1.53738318, -0.49861076],
        [-0.96924364, 1.87596750, 0.04155506],
        [0.05563008, -0.20397696, 1.05697151],
    ],
    dtype=np.float32,
)
SRGB_LUMA = np.array([0.2126, 0.7152, 0.0722], dtype=np.float32)
PIPELINE_VERSION = "jin-lumeo-multibrand-hdr-v1"


@dataclass(frozen=True)
class RawInfo:
    path: str
    brand: str
    suffix: str
    width: int
    height: int
    shutter_seconds: float
    iso: float
    aperture: float
    focal_length_mm: float
    lens: str
    white_balance: tuple[float, float, float, float]
    cfa_shape: tuple[int, int] | None
    demosaic: str

    @property
    def exposure_factor(self) -> float:
        aperture_sq = max(self.aperture * self.aperture, 1e-8)
        return self.shutter_seconds * max(self.iso, 1.0) / aperture_sq


class StopRequested(RuntimeError):
    pass


STOP_REQUESTED = False


def _request_stop(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def atomic_json(path: Path, value: dict[str, Any], *, keep_previous: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")
    if keep_previous and path.exists():
        previous = path.with_name(path.stem + ".previous" + path.suffix)
        shutil.copy2(path, previous)
    os.replace(temporary, path)


def sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def _portable_raw_identity(info: RawInfo, source_sha256: str) -> dict[str, Any]:
    """Content identity that remains valid after a checkpoint moves workers.

    Absolute scratch paths and filesystem mtimes are deliberately excluded.
    The object manifest verifies bytes before this pipeline starts, while the
    digest and immutable RAW characteristics prevent cross-input reuse.
    """
    value = asdict(info)
    value.pop("path", None)
    return {
        "filename": Path(info.path).name,
        "source_sha256": source_sha256,
        "raw": value,
    }


def decode_fingerprint(
    info: RawInfo,
    source_sha256: str,
    shared_wb: tuple[float, float, float, float],
) -> str:
    value = {
        "pipeline": PIPELINE_VERSION,
        "source": _portable_raw_identity(info, source_sha256),
        "demosaic": info.demosaic,
        "shared_wb": shared_wb,
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def classify_brand(path: Path) -> str:
    try:
        return SUPPORTED_RAW_EXTENSIONS[path.suffix.lower()]
    except KeyError as error:
        supported = ", ".join(sorted(SUPPORTED_RAW_EXTENSIONS))
        raise ValueError(f"Unsupported RAW extension {path.suffix!r}; supported: {supported}") from error


def _crop_geometry(raw: rawpy.RawPy) -> tuple[int, int, int, int]:
    sizes = raw.sizes
    x0 = max(0, int(sizes.crop_left_margin - sizes.left_margin))
    y0 = max(0, int(sizes.crop_top_margin - sizes.top_margin))
    width = int(sizes.crop_width or sizes.width)
    height = int(sizes.crop_height or sizes.height)
    width = min(width, int(sizes.width) - x0)
    height = min(height, int(sizes.height) - y0)
    return x0, y0, width, height


def inspect_raw(path: Path) -> RawInfo:
    brand = classify_brand(path)
    with rawpy.imread(str(path)) as raw:
        x0, y0, width, height = _crop_geometry(raw)
        pattern = raw.raw_pattern
        cfa_shape = tuple(map(int, pattern.shape)) if pattern is not None else None
        # LibRaw's automatic mode selects its X-Trans path for RAF. AAHD is kept
        # for Bayer files to match the user's previously accepted renders.
        demosaic = "libraw-auto-xtrans" if brand == "Fujifilm" or cfa_shape == (6, 6) else "AAHD"
        other = raw.other
        wb = tuple(float(v) for v in raw.camera_whitebalance)
        if len(wb) != 4 or not all(math.isfinite(v) and v > 0 for v in wb):
            raise ValueError(f"Invalid as-shot white balance in {path.name}")
        return RawInfo(
            path=str(path),
            brand=brand,
            suffix=path.suffix.lower(),
            width=width,
            height=height,
            shutter_seconds=float(other.shutter_speed),
            iso=float(other.iso_speed),
            aperture=float(other.aperture),
            focal_length_mm=float(other.focal_length),
            lens=raw.lens.model,
            white_balance=wb,
            cfa_shape=cfa_shape,
            demosaic=demosaic,
        )


def validate_bracket(infos: list[RawInfo]) -> None:
    if len(infos) < 2 or len(infos) > 20:
        raise ValueError("An HDR bracket must contain between 2 and 20 RAW files")
    brands = {info.brand for info in infos}
    if len(brands) != 1:
        raise ValueError(f"A bracket cannot mix camera brands: {sorted(brands)}")
    dimensions = {(info.width, info.height) for info in infos}
    if len(dimensions) != 1:
        raise ValueError(f"Bracket dimensions differ: {sorted(dimensions)}")
    if len({round(math.log2(max(info.exposure_factor, 1e-12)), 2) for info in infos}) == 1:
        raise ValueError("Bracket files do not contain distinct exposure values")


def validate_resource_envelope(
    infos: list[RawInfo],
    *,
    max_pixels: int | None,
    max_decompressed_bytes: int | None,
) -> None:
    pixels = infos[0].width * infos[0].height
    if max_pixels is not None and pixels > max_pixels:
        raise ValueError("RAW dimensions exceed the configured pixel limit")
    # Each decoded checkpoint is RGB uint16. This is a deterministic lower
    # bound, not a claim about peak working memory, which is separately capped.
    decoded_bytes = pixels * 3 * 2 * len(infos)
    if max_decompressed_bytes is not None and decoded_bytes > max_decompressed_bytes:
        raise ValueError("Decoded RAW checkpoints exceed the configured byte limit")


def smoothstep(values: np.ndarray, edge0: float, edge1: float) -> np.ndarray:
    x = np.clip((values - edge0) / (edge1 - edge0), 0.0, 1.0)
    return x * x * (3.0 - 2.0 * x)


def merge_rows(
    sensors: np.ndarray,
    scales: np.ndarray,
    valid: np.ndarray,
    shortest_index: int,
) -> np.ndarray:
    """Merge a row block using the user's established HDR weighting method."""
    radiances = sensors / scales[:, None, None, None]
    levels = np.max(sensors, axis=3)
    low_weight = smoothstep(levels, 0.002, 0.05)
    high_weight = 1.0 - smoothstep(levels, 0.70, 0.96)
    luminances = np.maximum(radiances[..., 1], 0.0)
    median_luminance = np.median(luminances, axis=0)
    stop_error = np.abs(
        np.log2((luminances + 1e-5) / (median_luminance[None, ...] + 1e-5))
    )
    deghost = np.exp(-((stop_error / 0.75) ** 4))
    preference = np.sqrt(scales)
    weights = low_weight * high_weight * deghost * preference[:, None, None]
    weights *= valid
    weight_sum = np.sum(weights, axis=0)
    merged = np.sum(radiances * weights[..., None], axis=0) / np.maximum(
        weight_sum[..., None], 1e-8
    )
    fallback = radiances[shortest_index]
    merged[weight_sum < 1e-8] = fallback[weight_sum < 1e-8]
    return merged.astype(np.float32)


def _atomic_tiff(path: Path, shape: tuple[int, int, int], dtype: Any, description: str) -> tuple[np.memmap, Path]:
    temporary = path.with_name(path.stem + ".tmp" + path.suffix)
    if temporary.exists():
        temporary.unlink()
    mapped = tifffile.memmap(
        temporary,
        shape=shape,
        dtype=dtype,
        photometric="rgb",
        metadata=None,
        description=description,
        software=f"{PIPELINE_VERSION}; rawpy {rawpy.__version__}; OpenCV {cv2.__version__}",
    )
    return mapped, temporary


def _finish_tiff(mapped: np.memmap, temporary: Path, final: Path) -> None:
    mapped.flush()
    del mapped
    os.replace(temporary, final)


def _decode_one(args: tuple[RawInfo, str, tuple[float, float, float, float], str]) -> tuple[str, dict[str, Any]]:
    info, checkpoint_dir_text, shared_wb, fingerprint = args
    source = Path(info.path)
    checkpoint_dir = Path(checkpoint_dir_text)
    output = checkpoint_dir / f"{source.stem}_linear_XYZ_D65_16bit.tif"
    metadata_path = output.with_suffix(".json")
    if output.exists() and metadata_path.exists():
        current = json.loads(metadata_path.read_text(encoding="utf-8"))
        if current.get("fingerprint") == fingerprint:
            return str(output), {"reused": True, **current}

    with rawpy.imread(str(source)) as raw:
        kwargs: dict[str, Any] = {
            "output_color": rawpy.ColorSpace.XYZ,
            "gamma": (1, 1),
            "output_bps": 16,
            "no_auto_bright": True,
            "user_wb": list(shared_wb),
            "user_flip": 0,
            "highlight_mode": rawpy.HighlightMode.Clip,
        }
        if info.demosaic == "AAHD":
            kwargs["demosaic_algorithm"] = rawpy.DemosaicAlgorithm.AAHD
        decoded = raw.postprocess(**kwargs)
        x0, y0, width, height = _crop_geometry(raw)
        decoded = decoded[y0 : y0 + height, x0 : x0 + width]

    output.parent.mkdir(parents=True, exist_ok=True)
    mapped, temporary = _atomic_tiff(
        output,
        decoded.shape,
        np.uint16,
        "Checkpoint: shared-WB scene-linear CIE XYZ D65; no auto brightness",
    )
    mapped[:] = decoded
    _finish_tiff(mapped, temporary, output)
    report = {
        "fingerprint": fingerprint,
        "source": info.path,
        "source_sha256": sha256(source),
        "brand": info.brand,
        "demosaic": info.demosaic,
        "shared_white_balance": list(shared_wb),
        "shape": list(decoded.shape),
    }
    atomic_json(metadata_path, report)
    return str(output), {"reused": False, **report}


def decode_checkpoints(
    infos: list[RawInfo], checkpoint_dir: Path, shared_wb: tuple[float, float, float, float], workers: int
) -> tuple[list[Path], list[dict[str, Any]]]:
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    jobs = []
    for info in infos:
        fingerprint = decode_fingerprint(info, sha256(Path(info.path)), shared_wb)
        jobs.append((info, str(checkpoint_dir), shared_wb, fingerprint))

    results: dict[str, tuple[Path, dict[str, Any]]] = {}
    if workers == 1:
        completed = map(_decode_one, jobs)
        for output, report in completed:
            results[report["source"]] = (Path(output), report)
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_decode_one, job) for job in jobs]
            for future in as_completed(futures):
                output, report = future.result()
                results[report["source"]] = (Path(output), report)

    return (
        [results[info.path][0] for info in infos],
        [results[info.path][1] for info in infos],
    )


def alignment_gray(image: np.ndarray) -> np.ndarray:
    y = image[..., 1].astype(np.float32) / 65535.0
    y = np.log1p(80.0 * np.maximum(y, 0.0))
    low, high = np.percentile(y, [1.0, 99.0])
    return np.asarray(np.clip((y - low) / max(high - low, 1e-6), 0.0, 1.0) * 255.0, dtype=np.uint8)


def estimate_alignment(
    images: list[np.ndarray], names: list[str], reference_index: int, scale: int = 4
) -> tuple[list[np.ndarray], list[dict[str, Any]]]:
    grays = [alignment_gray(np.asarray(image[::scale, ::scale])) for image in images]
    sift = cv2.SIFT_create(nfeatures=9000)
    features = [sift.detectAndCompute(gray, None) for gray in grays]
    reference_kp, reference_desc = features[reference_index]
    matrices: list[np.ndarray] = []
    reports: list[dict[str, Any]] = []
    for index, (keypoints, descriptors) in enumerate(features):
        if index == reference_index:
            matrix = np.eye(2, 3, dtype=np.float32)
            inliers = len(keypoints)
            matches = inliers
        else:
            if descriptors is None or reference_desc is None:
                raise RuntimeError(f"No alignment features in {names[index]}")
            pairs = cv2.BFMatcher(cv2.NORM_L2).knnMatch(descriptors, reference_desc, k=2)
            good = [match for match, other in pairs if match.distance < 0.7 * other.distance]
            source_points = np.float32([keypoints[m.queryIdx].pt for m in good])
            reference_points = np.float32([reference_kp[m.trainIdx].pt for m in good])
            small_matrix, mask = cv2.estimateAffinePartial2D(
                source_points,
                reference_points,
                method=cv2.RANSAC,
                ransacReprojThreshold=1.5,
                maxIters=5000,
                confidence=0.999,
                refineIters=30,
            )
            if small_matrix is None or mask is None or int(mask.sum()) < 100:
                raise RuntimeError(f"Alignment failed for {names[index]}")
            matrix = small_matrix.astype(np.float32)
            matrix[:, 2] *= scale
            inliers = int(mask.sum())
            matches = len(good)
        matrices.append(matrix)
        reports.append(
            {
                "source": names[index],
                "matches": matches,
                "inliers": inliers,
                "inlier_fraction": float(inliers / max(matches, 1)),
                "matrix_source_to_reference": matrix.tolist(),
            }
        )
    return matrices, reports


def warp_sources(
    images: list[np.ndarray], matrices: list[np.ndarray]
) -> tuple[list[np.ndarray], list[np.ndarray], np.ndarray]:
    height, width, _ = images[0].shape
    aligned: list[np.ndarray] = []
    valid_masks: list[np.ndarray] = []
    ones = np.ones((height, width), dtype=np.uint8)
    for image, matrix in zip(images, matrices):
        aligned.append(
            cv2.warpAffine(
                image,
                matrix,
                (width, height),
                flags=cv2.INTER_LINEAR,
                borderMode=cv2.BORDER_CONSTANT,
                borderValue=0,
            )
        )
        valid_masks.append(
            cv2.warpAffine(
                ones,
                matrix,
                (width, height),
                flags=cv2.INTER_NEAREST,
                borderMode=cv2.BORDER_CONSTANT,
                borderValue=0,
            ).astype(bool)
        )
    intersection = np.logical_and.reduce(valid_masks)
    return aligned, valid_masks, intersection


def estimate_exposure_scales(
    aligned: list[np.ndarray], intersection: np.ndarray, infos: list[RawInfo], shortest_index: int
) -> tuple[list[float], list[dict[str, Any]]]:
    reference = aligned[shortest_index].astype(np.float32) / 65535.0
    scales: list[float] = []
    reports: list[dict[str, Any]] = []
    reference_exposure = infos[shortest_index].exposure_factor
    for index, image_u16 in enumerate(aligned):
        metadata = infos[index].exposure_factor / reference_exposure
        if index == shortest_index:
            measured = 1.0
            samples = 0
            percentiles = [1.0] * 5
        else:
            image = image_u16.astype(np.float32) / 65535.0
            ratios = []
            for channel in range(3):
                mask = (
                    intersection
                    & (reference[..., channel] > 0.002)
                    & (reference[..., channel] < 0.12)
                    & (image[..., channel] > 0.015)
                    & (image[..., channel] < 0.72)
                )
                if np.count_nonzero(mask):
                    ratios.append(image[..., channel][mask] / reference[..., channel][mask])
            if not ratios:
                raise RuntimeError(f"No valid exposure samples for {Path(infos[index].path).name}")
            ratio = np.concatenate(ratios)
            measured = float(np.median(ratio))
            samples = int(ratio.size)
            percentiles = list(map(float, np.percentile(ratio, [5, 25, 50, 75, 95])))
            if not 0.35 * metadata <= measured <= 2.8 * metadata:
                raise RuntimeError(
                    f"Measured exposure ratio {measured:.3f} disagrees with metadata {metadata:.3f}"
                )
        scales.append(measured)
        reports.append(
            {
                "source": Path(infos[index].path).name,
                "measured_scale": measured,
                "metadata_scale": metadata,
                "difference_percent": 100.0 * (measured / metadata - 1.0),
                "samples": samples,
                "ratio_percentiles": percentiles,
            }
        )
    return scales, reports


def _state_fingerprint(infos: list[RawInfo], scales: Iterable[float], matrices: Iterable[np.ndarray]) -> str:
    value = {
        "pipeline": PIPELINE_VERSION,
        "sources": [
            _portable_raw_identity(info, sha256(Path(info.path))) for info in infos
        ],
        "scales": list(map(float, scales)),
        "matrices": [matrix.tolist() for matrix in matrices],
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def merge_hdr_resumable(
    aligned: list[np.ndarray],
    intersection: np.ndarray,
    scales: list[float],
    shortest_index: int,
    alignment_reference_index: int,
    checkpoint_dir: Path,
    fingerprint: str,
    *,
    chunk_rows: int = 160,
    stop_after_row: int | None = None,
) -> tuple[Path, dict[str, Any]]:
    final = checkpoint_dir / "HDR_reference_shortest_linear_XYZ_float32.tif"
    temporary = final.with_name(final.stem + ".inprogress" + final.suffix)
    state_path = checkpoint_dir / "merge_state.json"
    shape = aligned[0].shape
    start_row = 0
    if final.exists() and state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if (
            state.get("status") == "complete"
            and state.get("fingerprint") == fingerprint
            and tuple(state.get("shape", [])) == shape
        ):
            return final, {
                "resumed_from_row": shape[0],
                "rows": shape[0],
                "checkpoint": str(state_path),
                "reused_complete_result": True,
            }
    if temporary.exists() and state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("fingerprint") == fingerprint and tuple(state.get("shape", [])) == shape:
            start_row = int(state["next_row"])
            mapped = tifffile.memmap(temporary, mode="r+")
        else:
            temporary.unlink()
            mapped = tifffile.memmap(temporary, shape=shape, dtype=np.float32, photometric="rgb", metadata=None)
    else:
        if temporary.exists():
            temporary.unlink()
        mapped = tifffile.memmap(temporary, shape=shape, dtype=np.float32, photometric="rgb", metadata=None)

    scale_array = np.asarray(scales, dtype=np.float32)
    for top in range(start_row, shape[0], chunk_rows):
        bottom = min(shape[0], top + chunk_rows)
        sensors = np.stack(
            [image[top:bottom].astype(np.float32) / 65535.0 for image in aligned]
        )
        valid = np.broadcast_to(intersection[top:bottom][None, ...], sensors.shape[:3])
        merged = merge_rows(sensors, scale_array, valid, shortest_index)
        outside_common_frame = ~intersection[top:bottom]
        reference_radiance = sensors[alignment_reference_index] / scale_array[alignment_reference_index]
        merged[outside_common_frame] = reference_radiance[outside_common_frame]
        mapped[top:bottom] = merged
        mapped.flush()
        atomic_json(
            state_path,
            {
                "fingerprint": fingerprint,
                "shape": list(shape),
                "next_row": bottom,
                "total_rows": shape[0],
                "updated_at_unix": time.time(),
                "status": "running" if bottom < shape[0] else "merge_complete",
            },
            keep_previous=True,
        )
        if stop_after_row is not None and bottom >= stop_after_row:
            del mapped
            raise StopRequested(f"Intentional checkpoint stop after row {bottom}")
        if STOP_REQUESTED:
            del mapped
            raise StopRequested(f"Stop signal checkpointed after row {bottom}")

    mapped.flush()
    del mapped
    os.replace(temporary, final)
    atomic_json(
        state_path,
        {
            "fingerprint": fingerprint,
            "shape": list(shape),
            "next_row": shape[0],
            "total_rows": shape[0],
            "updated_at_unix": time.time(),
            "status": "complete",
            "final": str(final),
        },
        keep_previous=True,
    )
    return final, {
        "resumed_from_row": start_row,
        "rows": shape[0],
        "checkpoint": str(state_path),
        "reused_complete_result": False,
    }


def srgb_encode(linear: np.ndarray) -> np.ndarray:
    linear = np.maximum(linear, 0.0)
    return np.where(
        linear <= 0.0031308,
        12.92 * linear,
        1.055 * np.power(linear, 1.0 / 2.4) - 0.055,
    )


def gamut_compress(rgb: np.ndarray, ceiling: float = 0.995) -> np.ndarray:
    """Bring colors into display gamut without hard per-channel clipping."""
    luminance = np.maximum(rgb @ SRGB_LUMA, 0.0)
    low = np.min(rgb, axis=-1)
    scale = np.ones_like(luminance)
    negative = low < 0.0
    scale[negative] = luminance[negative] / np.maximum(
        luminance[negative] - low[negative], 1e-8
    )
    rgb = luminance[..., None] + (rgb - luminance[..., None]) * scale[..., None]

    luminance = np.clip(rgb @ SRGB_LUMA, 0.0, ceiling)
    high = np.max(rgb, axis=-1)
    scale.fill(1.0)
    above = high > ceiling
    scale[above] = (ceiling - luminance[above]) / np.maximum(
        high[above] - luminance[above], 1e-8
    )
    return np.clip(
        luminance[..., None] + (rgb - luminance[..., None]) * scale[..., None],
        0.0,
        ceiling,
    )


def render_balanced(hdr_path: Path, output_dir: Path, stem: str) -> dict[str, Any]:
    xyz = tifffile.memmap(hdr_path, mode="r")
    height, width, _ = xyz.shape
    rgb = np.empty(xyz.shape, dtype=np.float32)
    for top in range(0, height, 192):
        bottom = min(height, top + 192)
        rgb[top:bottom] = np.asarray(xyz[top:bottom], dtype=np.float32) @ XYZ_TO_SRGB.T

    luminance = np.maximum(rgb @ SRGB_LUMA, 1e-6)
    log_luminance = np.log2(luminance)
    scale = 8
    small = cv2.resize(
        log_luminance,
        (max(1, width // scale), max(1, height // scale)),
        interpolation=cv2.INTER_AREA,
    )
    base_small = cv2.GaussianBlur(small, (0, 0), sigmaX=28, sigmaY=28, borderType=cv2.BORDER_REFLECT)
    base = cv2.resize(base_small, (width, height), interpolation=cv2.INTER_CUBIC)
    detail = log_luminance - base
    center = float(np.median(base[::8, ::8]))
    mapped_log_luminance = (base - center) * 0.46 + detail * 0.90 + np.log2(0.26)
    mapped_luminance = np.exp2(mapped_log_luminance)
    mapped_luminance = mapped_luminance / (1.0 + mapped_luminance)
    rendered = rgb * (mapped_luminance / luminance)[..., None]
    rendered = srgb_encode(gamut_compress(rendered))
    output_u16 = np.rint(np.clip(rendered, 0.0, 1.0) * 65535.0).astype(np.uint16)

    profile = ImageCms.ImageCmsProfile(ImageCms.createProfile("sRGB")).tobytes()
    tiff_path = output_dir / f"{stem}_HDR_window-balanced_sRGB_16bit.tif"
    jpeg_path = output_dir / f"{stem}_HDR_window-balanced_sRGB_fullres.jpg"
    tifffile.imwrite(
        tiff_path,
        output_u16,
        photometric="rgb",
        metadata=None,
        description="16-bit sRGB; broad local tone compression for balanced interior and window detail",
        extratags=[(34675, "B", len(profile), profile, False)],
    )
    Image.fromarray((output_u16 / 257.0).astype(np.uint8)).save(
        jpeg_path, quality=96, subsampling=0, icc_profile=profile
    )
    sample = output_u16[::8, ::8]
    return {
        "tiff": str(tiff_path),
        "jpeg": str(jpeg_path),
        "black_fraction": float(np.mean(sample == 0)),
        "white_fraction": float(np.mean(sample == 65535)),
        "sample_percentiles": list(map(float, np.percentile(sample, [0.1, 1, 50, 99, 99.9]))),
    }


def run_pipeline(
    sources: list[Path],
    output_dir: Path,
    *,
    workers: int = 2,
    stop_after_row: int | None = None,
    max_pixels: int | None = None,
    max_decompressed_bytes: int | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = output_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    infos = [inspect_raw(path) for path in sources]
    validate_bracket(infos)
    validate_resource_envelope(
        infos,
        max_pixels=max_pixels,
        max_decompressed_bytes=max_decompressed_bytes,
    )
    shortest_index = int(np.argmin([info.exposure_factor for info in infos]))
    reference_index = int(np.argsort([info.exposure_factor for info in infos])[len(infos) // 2])
    shared_wb = infos[reference_index].white_balance
    decoded_paths, decode_reports = decode_checkpoints(infos, checkpoint_dir, shared_wb, workers)
    images = [tifffile.memmap(path, mode="r") for path in decoded_paths]
    matrices, alignment_reports = estimate_alignment(
        images, [Path(info.path).name for info in infos], reference_index
    )
    aligned, _valid_masks, intersection = warp_sources(images, matrices)
    scales, exposure_reports = estimate_exposure_scales(aligned, intersection, infos, shortest_index)
    fingerprint = _state_fingerprint(infos, scales, matrices)
    hdr_path, resume_report = merge_hdr_resumable(
        aligned,
        intersection,
        scales,
        shortest_index,
        reference_index,
        checkpoint_dir,
        fingerprint,
        stop_after_row=stop_after_row,
    )
    stem = f"{sources[0].stem}-{sources[-1].stem}"
    render_report = render_balanced(hdr_path, output_dir, stem)
    report = {
        "status": "complete",
        "pipeline": PIPELINE_VERSION,
        "elapsed_seconds": time.perf_counter() - started,
        "brand": infos[0].brand,
        "supported_brands": sorted(set(SUPPORTED_RAW_EXTENSIONS.values())),
        "sources": [asdict(info) for info in infos],
        "shortest_exposure_index": shortest_index,
        "alignment_reference_index": reference_index,
        "shared_white_balance": list(shared_wb),
        "decode": decode_reports,
        "alignment": alignment_reports,
        "exposure": exposure_reports,
        "measured_scales": scales,
        "merge": {"linear_xyz_float32": str(hdr_path), **resume_report},
        "render": render_report,
        "quality_gates": {
            "all_alignment_inliers_at_least_100": all(item["inliers"] >= 100 for item in alignment_reports),
            "no_exact_white_sample": render_report["white_fraction"] == 0.0,
            "output_dimensions_preserved": list(tifffile.memmap(hdr_path, mode="r").shape[:2])
            == [infos[0].height, infos[0].width],
        },
        "color_note": (
            "LibRaw camera matrices and one shared as-shot WB are used. This is deterministic and prevents "
            "frame-to-frame color shifts; mixed illuminants still require a reference target or local grading "
            "for metrologically exact color."
        ),
    }
    manifest = output_dir / f"{stem}_HDR_manifest.json"
    atomic_json(manifest, report)
    report["manifest"] = str(manifest)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources", nargs="+", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--stop-after-row", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--max-pixels", type=int, default=None)
    parser.add_argument("--max-decompressed-bytes", type=int, default=None)
    return parser


def main() -> None:
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    args = build_parser().parse_args()
    sources = [path.resolve() for path in args.sources]
    for source in sources:
        if not source.is_file():
            raise FileNotFoundError(source)
    workers = max(1, min(args.workers, len(sources), 4))
    try:
        report = run_pipeline(
            sources,
            args.output_dir.resolve(),
            workers=workers,
            stop_after_row=args.stop_after_row,
            max_pixels=args.max_pixels,
            max_decompressed_bytes=args.max_decompressed_bytes,
        )
    except StopRequested as error:
        print(json.dumps({"status": "checkpointed", "reason": str(error)}, ensure_ascii=False))
        raise SystemExit(75) from error
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
