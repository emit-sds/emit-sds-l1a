"""
This script takes in a reassembled raw ENVI image and writes out the timing of the line timestamps for each line.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import logging
import numpy as np
import os

from argparse import RawTextHelpFormatter
from spectral.io import envi

from emit_sds_l1a.ccsds_packet import SciencePacketProcessor
from emit_sds_l1a.frame import Frame

MAX_32BIT_UNSIGNED_INT = 4294967295


def calculate_seconds_since_gps_epoch(line_timestamp, os_time_timestamp, os_time):
    # Need to adjust line timestamp in the case where the clock rolls over (which happens about every 12 hours)
    if line_timestamp < os_time_timestamp:
        line_timestamp = line_timestamp + MAX_32BIT_UNSIGNED_INT
    # timestamp counter runs at 100,000 ticks per second.
    # convert to nanoseconds by dividing by 10^5 and then multiplying by 10^9 (or just multiply by 10^4)
    line_offset_nanoseconds = (line_timestamp - os_time_timestamp) * 10 ** 4
    # OS time is in nanoseconds since GPS epoch
    return os_time + line_offset_nanoseconds

def main():

    # Read in args
    parser = argparse.ArgumentParser(
        description="Description: This script creates a line timestamps file given a raw image cube.\n"
                    "Operating Environment: Python 3.x. See setup.py file for specific dependencies.\n"
                    "Outputs:\n"
                    "    * Text file containing the time of each line in the raw image as units of nanoseconds since "
                    "the GPS epoch\n",
        formatter_class=RawTextHelpFormatter)
    parser.add_argument("raw_path", help="Path to the reassembled raw file")
    parser.add_argument("os_time_timestamp", help="The OS time timestamp to use to calculate timing")
    parser.add_argument("os_time", help="The OS time in nanoseconds since GPS epoch to use to calculate timing")
    parser.add_argument("--work_dir", help="Path to working directory", default=".")
    parser.add_argument("--level", help="Logging level", default="INFO")
    parser.add_argument("--log_path", help="Path to log file", default="generate_line_timestamps.log")

    args = parser.parse_args()

    # Format args as needed
    args.level = args.level.upper()

    if not os.path.exists(args.work_dir):
        os.makedirs(args.work_dir)
    output_dir = os.path.join(args.work_dir, "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Set up console logging using root logger
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    logger = logging.getLogger("emit-sds-l1a")

    # Set up file handler logging
    handler = logging.FileHandler(args.log_path)
    handler.setLevel(args.level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info(f"Opening raw image cube at {args.raw_path}. Using OS time timestamp {args.os_time_timestamp}, and "
                f"OS time {args.os_time}")

    # Read in raw image
    hdr_path = args.raw_path.replace(".img", ".hdr") if args.raw_path.endswith(".img") else args.raw_path + ".hdr"
    hdr = envi.read_envi_header(hdr_path)
    lines = int(hdr['lines'])
    bands = int(hdr['bands'])
    samples = int(hdr['samples'])
    img = np.memmap(args.raw_path, shape=(lines, bands, samples), dtype=np.uint16, mode="r")
    line_headers = img[:, 0, :]

    # TODO: What does missing data look like?
    timestamps = []
    for i in range(lines):
        hdr = bytearray(line_headers[i, :])
        line_timestamp = int.from_bytes(hdr[0:4], byteorder="little", signed=False)
        timestamps.append(line_timestamp)

    logger.info("Done.")


if __name__ == '__main__':
    main()
