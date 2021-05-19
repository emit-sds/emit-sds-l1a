"""
This script reassembles the raw image cube from acquisition frames with uncompressed frame headers and compressed
frame data.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import glob
import logging
import os
import subprocess
import sys

import numpy as np
import spectral.io.envi as envi

from emit_sds_l1a.frame import Frame

# TODO: Different flags for missing frame vs. missing portion of frame?
MISSING_DATA_FLAG = -9998
CLOUDY_DATA_FLAG = -9997


def main():

    # Read in args
    parser = argparse.ArgumentParser()
    parser.add_argument("comp_frames_dir", help="Compressed frames directory path")
    parser.add_argument("--flexcodec_exe", help="Path to flexcodec exe")
    parser.add_argument("--constants_path", help="Path to constants.txt file")
    parser.add_argument("--init_data_path", help="Path to init_data.bin file")
    parser.add_argument("--out_dir", help="Output directory", default=".")
    parser.add_argument("--level", help="Logging level", default="INFO")
    parser.add_argument("--log_path", help="Path to log file")

    args = parser.parse_args()

    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    # Set up console logging using root logger
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    logger = logging.getLogger("emit-sds-l1a")
    # Set up file handler logging
    if args.log_path is not None:
        handler = logging.FileHandler(args.log_path)
        handler.setLevel(args.level)
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Process frame headers and write out compressed data files
    comp_frame_paths = glob.glob(os.path.join(args.comp_frames_dir, "*"))
    comp_frame_paths.sort()
    decomp_frame_paths = []
    for path in comp_frame_paths:
        logger.info(f"Reading in frame {path}")
        with open(path, "rb") as f:
            frame_binary = f.read()
        frame = Frame(frame_binary)
        # TODO: Process frame header and create report?
        # Write out frame data section containing compressed data
        comp_data_path = os.path.join(args.out_dir, os.path.basename(path))
        frame.write_data(comp_data_path)
        # Decompress frame
        cmd = [args.flexcodec_exe, comp_data_path, "-v", "-a", args.constants_path, "-i", args.init_data_path,
               "--no-headers", "--bil"]
        cmd_str = " ".join(cmd)
        logger.info(f"Decompressing frame with command '{cmd_str}'")
        output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
        logger.info(output.stdout.decode("utf-8").replace("\\n", "\n").replace("\\t", "\t"))
        if output.returncode != 0:
            logging.error(f"Failed to decompress frame with command '{cmd_str}'")
            raise RuntimeError(output.stderr.decode("utf-8"))
        decomp_frame_paths.append(comp_data_path + ".decomp")

    # Add empty decompressed frame files to fill in missing frame numbers
    decomp_frame_nums = [int(os.path.basename(path).split("_")[1]) for path in decomp_frame_paths]
    seq_frame_nums = list(range(decomp_frame_nums[0], decomp_frame_nums[0] + len(decomp_frame_nums)))
    missing_frame_nums = list(set(seq_frame_nums) - set(decomp_frame_nums))
    logger.debug(f"List of missing frame numbers (if any): {missing_frame_nums}")
    acquisition_id = os.path.basename(decomp_frame_paths[0].split("_")[0])
    for frame_num in missing_frame_nums:
        decomp_frame_paths.append(os.path.join(args.out_dir, "_".join([acquisition_id, str(frame_num).zfill(5), "6"])))
    decomp_frame_paths.sort()

    # Reassemble frames into ENVI image cube filling in missing and cloudy data with data flags
    # TODO: Look at submode flag to identify raw vs dark
    hdr_path = os.path.join(args.out_dir, acquisition_id + "_raw.hdr")
    num_lines = 32 * (len(decomp_frame_paths) - 1)
    hdr = {
        "description": "EMIT L1A raw instrument data (units: DN)",
        "samples": 1280,
        "lines": num_lines,
        "bands": 328,
        "header offset": 0,
        "file type": "ENVI",
        "data type": 2,
        "interleave": "bil",
        "byte order": 0
    }

    envi.write_envi_header(hdr_path, hdr)
    out_file = envi.create_image(hdr_path, hdr, ext="img", force=True)
    output = out_file.open_memmap(interleave="source", writable=True)
    # TODO: initialize output to some value here?  how do I handle missing/corrupt data?
    output[:, :, :] = -9999

    logger.debug(f"Assembling frames into raw file with header {hdr_path}")
    line = 0
    for path in decomp_frame_paths:
        status = int(os.path.basename(path).split(".")[0].split("_")[2])
        # Non-cloudy frames
        if status in (0, 1):
            frame = np.memmap(path, shape=(32, int(hdr["bands"]), int(hdr["samples"])), dtype=np.uint16, mode="r")
            output[line:line + 32, :, :] = frame[:, :, :].copy()
        # Cloudy frames
        if status in (4, 5):
            frame = np.full(shape=(32, int(hdr["bands"]), int(hdr["samples"])), fill_value=CLOUDY_DATA_FLAG, dtype=np.uint16)
            output[line:line + 32, :, :] = frame[:, :, :].copy()
        # Missing frames
        if status == 6:
            frame = np.full(shape=(32, int(hdr["bands"]), int(hdr["samples"])), fill_value=MISSING_DATA_FLAG, dtype=np.uint16)
            output[line:line + 32, :, :] = frame[:, :, :].copy()
        line += 32
    del output

    logger.info("Done")


if __name__ == '__main__':
    main()
