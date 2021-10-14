"""
This script takes in a ccsds packet stream and writes out the science frames embedded within.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import logging
import os

from argparse import RawTextHelpFormatter

from emit_sds_l1a.ccsds_packet import SciencePacketProcessor
from emit_sds_l1a.frame import Frame


def main():

    # Read in args
    parser = argparse.ArgumentParser(
        description="Description: This script executes the depacketization portion of the L1A PGE.\n"
                    "Operating Environment: Python 3.x. See setup.py file for specific dependencies.\n"
                    "Outputs:\n"
                    "    * List of frames named <DCID>_<frame_num>_<expected_number_of_frames>_<acquisition_status>\n"
                    "    * PGE log file named depacketize_science_frames.log (default)\n"
                    "    * Depacketization summary/report file named depacketize_science_frames_report.txt (default)\n",
        formatter_class=RawTextHelpFormatter)
    parser.add_argument("stream_path", help="Path to CCSDS stream file")
    parser.add_argument("--out_dir", help="Path to output directory", default=".")
    parser.add_argument("--level", help="Logging level", default="INFO")
    parser.add_argument("--log_path", help="Path to log file", default="depacketize_science_frames.log")
    parser.add_argument("--test_mode", action="store_true",
                        help="If enabled, some checking will be disabled like calculating checksums")

    args = parser.parse_args()

    # Upper case the log level
    args.level = args.level.upper()

    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    # Set up console logging using root logger
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    logger = logging.getLogger("emit-sds-l1a")

    # Set up file handler logging
    handler = logging.FileHandler(args.log_path)
    handler.setLevel(args.level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info(f"Processing stream file {args.stream_path}")
    processor = SciencePacketProcessor(args.stream_path, args.test_mode)

    while True:
        try:
            frame_binary = processor.read_frame()
            frame = Frame(frame_binary)
            frame.save(args.out_dir, args.test_mode)
        except EOFError:
            break

    report_path = args.log_path.replace(".log", "_report.txt")
    processor.stats(out_file=report_path)


if __name__ == '__main__':
    main()
