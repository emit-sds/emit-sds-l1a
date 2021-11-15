"""
This script takes in a reassembled raw ENVI image and writes out the timing of the line timestamps for each line.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import logging
import numpy as np
import os

from argparse import RawTextHelpFormatter
from spectral.io import envi

from ait.core import dmc

NUM_32_BIT_UINTS = 4294967296


def calculate_nanoseconds_since_gps_epoch(line_timestamp, os_time_timestamp, os_time):
    # Need to adjust line timestamp in the case where the clock rolls over (which happens about every 12 hours)
    if line_timestamp < os_time_timestamp:
        line_timestamp = line_timestamp + NUM_32_BIT_UINTS
    # timestamp counter runs at 100,000 ticks per second.
    # convert to nanoseconds by dividing by 10^5 and then multiplying by 10^9 (or just multiply by 10^4)
    line_offset_nanoseconds = (line_timestamp - os_time_timestamp) * 10 ** 4
    # OS time is in nanoseconds since GPS epoch
    return os_time + line_offset_nanoseconds


def get_utc_time_from_gps(gps_time):
    # Convert gps_time in nanoseconds to a timestamp in utc
    d = dmc.GPS_Epoch + dt.timedelta(seconds=(gps_time / 10 ** 9))
    offset = dmc.LeapSeconds.get_GPS_offset_for_date(d)
    utc_time = d - dt.timedelta(seconds=offset)
    return utc_time


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
    parser.add_argument("os_time_timestamp", type=int, help="The OS time timestamp to use to calculate timing")
    parser.add_argument("os_time", type=int,
                        help="The OS time in nanoseconds since GPS epoch to use to calculate timing")
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

    # Construct output path
    output_path = os.path.join(output_dir, os.path.basename(args.raw_path).split("_")[0] + "_line_timestamps.txt")

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
    img = np.memmap(args.raw_path, shape=(lines, bands, samples), dtype=np.int16, mode="r")
    line_headers = img[:, 0, :]

    bad_data_flags = []
    out_arr = []
    logger.info(f"Writing line timestamps to output file {output_path}")
    out_file = open(output_path, "w")
    for i in range(lines):
        # Get line header and convert to byte array
        line_hdr = line_headers[i, :]
        line_hdr_bytes = bytearray(line_hdr)
        # Get average of line header to determine if it is missing or cloudy data
        line_hdr_avg = sum(line_hdr) / len(line_hdr)
        bad_data_flags.append(line_hdr_avg)
        # Populate timing array (seconds since GPS
        if line_hdr_avg in [-9998.0, -9997.0]:
            # Use -1 to indicate no data
            out_arr.append([i, -1, "00000000T000000.000", -1, -1])
            out_file.write(f"{str(i).zfill(6)} {str(-1).zfill(19)} 0000-00-00T00:00:00.000000 {str(-1).zfill(10)} "
                           f"{str(-1).zfill(10)}\n")
        else:
            line_timestamp = int.from_bytes(line_hdr_bytes[0:4], byteorder="little", signed=False)
            line_count = int.from_bytes(line_hdr_bytes[4:8], byteorder="little", signed=False)
            nanosecs_since_gps = calculate_nanoseconds_since_gps_epoch(
                line_timestamp=line_timestamp,
                os_time_timestamp=args.os_time_timestamp,
                os_time=args.os_time
            )
            utc_time_str = get_utc_time_from_gps(nanosecs_since_gps).strftime("%Y-%m-%dT%H:%M:%S.%f")
            out_arr.append([i, nanosecs_since_gps, utc_time_str, line_timestamp, line_count])
            out_file.write(f"{str(i).zfill(6)} {str(nanosecs_since_gps).zfill(19)} "
                           f"{utc_time_str} {str(line_timestamp).zfill(10)} {str(line_count).zfill(10)}\n")

    logger.info("Done.")


if __name__ == '__main__':
    main()
