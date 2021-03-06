"""
Unit test for checking the reassemble_raw_cube.py script

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os
import subprocess


def test_reassemble_raw():

    print("Running test_reassemble_raw")

    test_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(test_dir, "data")
    repo_dir = os.path.dirname(test_dir)
    reassemble_raw_exe = os.path.join(repo_dir, "reassemble_raw_cube.py")
    compressed_frames_dir = os.path.join(data_dir, "compressed_frames")
    flex_exe = os.path.join(os.path.dirname(repo_dir), "EMIT_FLEX_codec", "flexcodec")
    constants_txt = os.path.join(data_dir, "compression_inputs", "constants.txt")
    init_data_bin = os.path.join(data_dir, "compression_inputs",
                                 "FPGA_Data_Initialization_File_CREATE_COMPRESSION_INIT_DATA_328_e0.bin")
    work_dir = os.path.join(test_dir, "work_reassemble_raw")
    log_path = os.path.join(work_dir, "test_run.log")

    cmd = ["python", reassemble_raw_exe, compressed_frames_dir,
           "--flexcodec_exe", flex_exe,
           "--constants_path", constants_txt,
           "--init_data_path", init_data_bin,
           "--work_dir", work_dir,
           "--log_path", log_path,
           "--level", "DEBUG",
           "--test_mode"]

    output = subprocess.run(" ".join(cmd), shell=True, capture_output=True, env=os.environ.copy())
    if output.returncode != 0:
        print(output.stderr.decode("utf-8"))
    if output.returncode == 0:
        os.system(f"rm -rf {work_dir}")
    assert output.returncode == 0
