"""
Unit test for checking the reassemble_raw_cube.py script

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os
import subprocess


def test_reassemble_raw():

    print("Running test_reassemble_raw")

    test_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(test_dir)
    reassemble_raw_exe = os.path.join(base_dir, "reassemble_raw_cube.py")
    compressed_frames_dir = os.path.join(test_dir, "compressed_frames")
    flex_exe = os.path.join(os.path.dirname(base_dir), "EMIT_FLEX_codec", "flexcodec")
    constants_txt = os.path.join(test_dir, "compression_inputs", "constants.txt")
    init_data_bin = os.path.join(test_dir, "compression_inputs", "init_data.bin")
    out_dir = os.path.join(test_dir, "out")
    log_path = os.path.join(out_dir, "test_run.log")

    cmd = ["python", reassemble_raw_exe, compressed_frames_dir,
           "--flexcodec_exe", flex_exe,
           "--constants_path",constants_txt,
           "--init_data_path", init_data_bin,
           "--out_dir", out_dir,
           "--log_path", log_path]

    output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
    if output.returncode != 0:
        print(output.stderr.decode("utf-8"))
    if output.returncode == 0:
        os.system(f"rm -rf {out_dir}")
    assert output.returncode == 0


test_reassemble_raw()