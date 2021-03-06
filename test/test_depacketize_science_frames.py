"""
Unit test for checking the depacketize_science_frames.py script

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os
import subprocess


def test_depacketize_science_frames():

    print("Running test_depacketize_science_frames")

    test_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(test_dir, "data")
    repo_dir = os.path.dirname(test_dir)
    depacketize_exe = os.path.join(repo_dir, "depacketize_science_frames.py")
    stream_path = os.path.join(data_dir, "ccsds", "0014_00_ccsds.bin")
    work_dir = os.path.join(test_dir, "work_depacketize_science_frames")
    log_path = os.path.join(work_dir, "test_run.log")

    cmd = ["python", depacketize_exe, stream_path,
           "--work_dir", work_dir,
           "--log_path", log_path,
           "--level", "DEBUG"]

    output = subprocess.run(" ".join(cmd), shell=True, capture_output=True, env=os.environ.copy())
    if output.returncode != 0:
        print(output.stderr.decode("utf-8"))
    if output.returncode == 0:
        os.system(f"rm -rf {work_dir}")
    assert output.returncode == 0
