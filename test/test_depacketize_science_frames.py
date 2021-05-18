"""
Unit test for checking the depacketize_science_frames.py script

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os
import subprocess


def test_depacketize_science_frames():

    print("Running test_depacketize_science_frames")

    test_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(test_dir)
    depacketize_exe = os.path.join(base_dir, "depacketize_science_frames.py")
    stream_path = os.path.join(test_dir, "ccsds", "0004_00_ccsds.bin")
    out_dir = os.path.join(test_dir, "out")
    log_path = os.path.join(out_dir, "test_run.log")

    cmd = ["python", depacketize_exe, stream_path,
           "--out_dir", out_dir,
           "--log_path", log_path]

    output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
    if output.returncode != 0:
        print(output.stderr.decode("utf-8"))
    if output.returncode == 0:
        os.system(f"rm -rf {out_dir}")
    assert output.returncode == 0
