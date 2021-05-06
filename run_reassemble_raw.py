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

from emit_sds_l1a.frame import Frame


def main():

    # Read in args
    parser = argparse.ArgumentParser()
    parser.add_argument("comp_frames_dir", help="Compressed frames directory path")
    parser.add_argument("flexcodec_exe", help="Path to flexcodec exe")
    parser.add_argument("constants_path", help="Path to constants.txt file")
    parser.add_argument("init_data_path", help="Path to init_data.bin file")
    parser.add_argument("--out_dir", help="Output directory", default=".")
    parser.add_argument("--level", help="Logging level", default="DEBUG")
    parser.add_argument("--log_path", help="Path to log file", default="reassemble_raw.log")

    args = parser.parse_args()

    # Set up logging
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    if args.log_path is None:
        logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    else:
        logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level, filename=args.log_path)

    # Process frame headers and write out compressed data files
    comp_frame_paths = glob.glob(os.path.join(args.comp_frames_dir, "*"))
    comp_frame_paths.sort()
    for path in comp_frame_paths:
        with open(path, "rb") as f:
            frame_binary = f.read()
        frame = Frame(frame_binary)
        # TODO: Process frame header and do what?
        # Write out frame data section containing compressed data
        comp_data_path = os.path.join(args.out_dir, os.path.basename(path))
        frame.write_data(comp_data_path)
        # Decompress frame
        cmd = [args.flexcodec_exe, comp_data_path, "-v", "-a", args.constants_path, "-i", args.init_data_path,
               "--no-headers", "--bil"]
        cmd_str = " ".join(cmd)
        logging.info(f"Decompressing frame with command '{cmd_str}'")
        output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)
        if output.returncode != 0:
            logging.error(f"Failed to decompress frame with command '{cmd_str}'")
            raise RuntimeError(output.stderr.decode("utf-8"))

    logging.info("Done")


if __name__ == '__main__':
    main()
