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
    parser.add_argument("--work_dir", help="Path to working directory", default=".")
    parser.add_argument("--prev_stream_path", help="Path to previous CCSDS stream file")
    parser.add_argument("--prev_bytes_to_read", help="How many bytes to read from the end of the previous stream",
                        default="40000000")
    parser.add_argument("--level", help="Logging level", default="INFO")
    parser.add_argument("--log_path", help="Path to log file", default="depacketize_science_frames.log")

    args = parser.parse_args()

    # Format args as needed
    args.level = args.level.upper()
    args.prev_bytes_to_read = int(args.prev_bytes_to_read)

    if not os.path.exists(args.work_dir):
        os.makedirs(args.work_dir)
    frames_dir = os.path.join(args.work_dir, "frames")
    if not os.path.exists(frames_dir):
        os.makedirs(frames_dir)

    # Set up console logging using root logger
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    logger = logging.getLogger("emit-sds-l1a")

    # Set up file handler logging
    handler = logging.FileHandler(args.log_path)
    handler.setLevel(args.level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Handle previous stream path if it exists
    prev_stream = bytearray()
    if args.prev_stream_path is not None:
        with open(args.prev_stream_path, "rb") as f:
            bytes_to_read = min(args.prev_bytes_to_read, os.path.getsize(args.prev_stream_path))
            f.seek(-bytes_to_read, 2)
            prev_stream = f.read(bytes_to_read)

    # Prepend previous stream bytes and write tmp file if needed
    if len(prev_stream) == 0:
        tmp_stream_path = args.stream_path
    else:
        logger.info(f"Prepending last {len(prev_stream)} bytes from {args.prev_stream_path} to {args.stream_path}")
        stream = prev_stream + open(args.stream_path, "rb").read()
        in_file_base = os.path.basename(args.stream_path).split(".")[0]
        prev_file_base = os.path.basename(args.prev_stream_path).split(".")[0]
        tmp_stream_path = os.path.join(args.work_dir, prev_file_base + "_" + in_file_base + ".bin")
        with open(tmp_stream_path, "wb") as f:
            f.write(stream)

    logger.info(f"Processing stream file {tmp_stream_path}")
    processor = SciencePacketProcessor(tmp_stream_path)

    frame_count = 0
    while True:
        try:
            frame_binary = processor.read_frame()
            frame = Frame(frame_binary)
            frame.save(frames_dir)
            frame_count += 1
        except EOFError:
            break

    logger.info(f"Total depacketized frames in stream file: {frame_count}")
    report_path = args.log_path.replace(".log", "_report.txt")
    processor.stats(out_file=report_path)


if __name__ == '__main__':
    main()
