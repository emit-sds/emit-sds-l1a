"""
This script takes in a ccsds packet stream and writes out the science frames embedded within.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import logging
import os

from emit_sds_l1a.ccsds_packet import SciencePacketProcessor
from emit_sds_l1a.frame import Frame


def main():

    # Read in args
    parser = argparse.ArgumentParser()
    parser.add_argument("stream_path", help="Path to CCSDS stream file")
    parser.add_argument("--out_dir", help="Path to output directory", default=".")
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

    logger.info(f"Processing stream file {args.stream_path}")
    processor = SciencePacketProcessor(args.stream_path)

    while True:
        try:
            frame_binary = processor.read_frame()
            frame = Frame(frame_binary)
            frame.save(args.out_dir)
        except EOFError:
            break

    report_path = os.path.join(args.out_dir, "depacketization_stats.txt")
    processor.stats(out_file=report_path)


if __name__ == '__main__':
    main()
