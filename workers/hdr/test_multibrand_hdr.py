from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np
import tifffile


MODULE_PATH = Path(__file__).with_name("multibrand_hdr.py")
SPEC = importlib.util.spec_from_file_location("multibrand_hdr", MODULE_PATH)
assert SPEC and SPEC.loader
hdr = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = hdr
SPEC.loader.exec_module(hdr)


class MultiBrandHdrTests(unittest.TestCase):
    @staticmethod
    def raw_info(path: Path) -> object:
        return hdr.RawInfo(
            path=str(path),
            brand="Canon",
            suffix=".cr3",
            width=24,
            height=16,
            shutter_seconds=0.01,
            iso=100.0,
            aperture=8.0,
            focal_length_mm=24.0,
            lens="fixture",
            white_balance=(2.0, 1.0, 1.5, 1.0),
            cfa_shape=(2, 2),
            demosaic="AAHD",
        )

    def test_supported_camera_extensions(self) -> None:
        expected = {
            "test.cr3": "Canon",
            "test.cr2": "Canon",
            "test.nef": "Nikon",
            "test.nrw": "Nikon",
            "test.arw": "Sony",
            "test.raf": "Fujifilm",
        }
        for filename, brand in expected.items():
            with self.subTest(filename=filename):
                self.assertEqual(hdr.classify_brand(Path(filename)), brand)

    def test_bracket_contract_accepts_two_to_twenty_exposures(self) -> None:
        base = self.raw_info(Path("fixture.CR3"))
        for count in (2, 3, 4, 5, 7, 9, 20):
            infos = [replace(base, path=f"{index}.CR3", shutter_seconds=0.001 * (2 ** index)) for index in range(count)]
            hdr.validate_bracket(infos)
        for count in (1, 21):
            infos = [replace(base, path=f"{index}.CR3", shutter_seconds=0.001 * (2 ** index)) for index in range(count)]
            with self.assertRaisesRegex(ValueError, "between 2 and 20"):
                hdr.validate_bracket(infos)

    def test_previous_merge_equation_recovers_radiance(self) -> None:
        rng = np.random.default_rng(20260728)
        radiance = rng.uniform(0.01, 0.09, size=(12, 16, 3)).astype(np.float32)
        scales = np.array([1.0, 4.0, 12.0], dtype=np.float32)
        sensors = np.clip(radiance[None, ...] * scales[:, None, None, None], 0, 1)
        valid = np.ones(sensors.shape[:3], dtype=bool)
        merged = hdr.merge_rows(sensors, scales, valid, shortest_index=0)
        np.testing.assert_allclose(merged, radiance, atol=2e-6, rtol=2e-5)

    def test_interrupted_resume_equals_uninterrupted(self) -> None:
        rng = np.random.default_rng(51)
        radiance = rng.uniform(0.001, 0.2, size=(31, 24, 3)).astype(np.float32)
        scales = [1.0, 3.8, 11.7]
        aligned = [
            np.rint(np.clip(radiance * scale, 0, 1) * 65535).astype(np.uint16)
            for scale in scales
        ]
        valid = np.ones(radiance.shape[:2], dtype=bool)
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            full_path, _ = hdr.merge_hdr_resumable(
                aligned, valid, scales, 0, 1, Path(a), "same", chunk_rows=7
            )
            with self.assertRaises(hdr.StopRequested):
                hdr.merge_hdr_resumable(
                    aligned, valid, scales, 0, 1, Path(b), "same", chunk_rows=7, stop_after_row=10
                )
            resumed_path, report = hdr.merge_hdr_resumable(
                aligned, valid, scales, 0, 1, Path(b), "same", chunk_rows=7
            )
            self.assertGreater(report["resumed_from_row"], 0)
            np.testing.assert_array_equal(
                tifffile.imread(full_path), tifffile.imread(resumed_path)
            )

            reused_path, reused_report = hdr.merge_hdr_resumable(
                aligned, valid, scales, 0, 1, Path(b), "same", chunk_rows=7
            )
            self.assertEqual(reused_path, resumed_path)
            self.assertTrue(reused_report["reused_complete_result"])
            self.assertEqual(reused_report["resumed_from_row"], radiance.shape[0])

    def test_checkpoint_identity_survives_worker_path_and_mtime_changes(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_path = Path(first) / "same.CR3"
            second_path = Path(second) / "same.CR3"
            first_path.write_bytes(b"same-object-content")
            second_path.write_bytes(b"same-object-content")
            os.utime(first_path, ns=(1_000_000_000, 1_000_000_000))
            os.utime(second_path, ns=(9_000_000_000, 9_000_000_000))
            info_a = self.raw_info(first_path)
            info_b = self.raw_info(second_path)
            shared = (2.0, 1.0, 1.5, 1.0)
            self.assertEqual(
                hdr.decode_fingerprint(info_a, hdr.sha256(first_path), shared),
                hdr.decode_fingerprint(info_b, hdr.sha256(second_path), shared),
            )
            scales = [1.0]
            matrices = [np.eye(2, 3, dtype=np.float32)]
            self.assertEqual(
                hdr._state_fingerprint([info_a], scales, matrices),
                hdr._state_fingerprint([info_b], scales, matrices),
            )

    def test_actual_raw_dimensions_enforce_pixel_and_decompressed_limits(self) -> None:
        info = self.raw_info(Path("fixture.CR3"))
        with self.assertRaisesRegex(ValueError, "pixel limit"):
            hdr.validate_resource_envelope([info], max_pixels=100, max_decompressed_bytes=1_000_000)
        with self.assertRaisesRegex(ValueError, "Decoded RAW"):
            hdr.validate_resource_envelope([info, info], max_pixels=1_000, max_decompressed_bytes=2_000)


if __name__ == "__main__":
    unittest.main()
