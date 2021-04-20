"""
The Frame class is used to read acquisition frames with uncompressed frame headers and compressed frame data.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import csv
import logging
import os
import sys

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
logger = logging.getLogger("emit-sds-l1a")

HDR_NUM_BYTES = 1280


class Frame:

    def __init__(self, frame_path=None):
        logger.debug("Initializing frame from path %s" % frame_path)
        with open(frame_path, "rb") as f:
            frame = f.read()
        self.hdr = frame[0: 1280]
        data_size = int.from_bytes(self.hdr[4:8], byteorder="little", signed=False)
        self.data = frame[1280: 1280 + data_size]
        self.dcid = self.hdr[28:32].decode("utf-8")
        self.frame_num = int.from_bytes(self.hdr[8:16], byteorder="little", signed=False)

    def write_data(self, data_dir):
        # Lookup start time using DCID
        start_time_str = ""
        csv_path = os.path.join(data_dir, "dcid_lookup.csv")
        with open(csv_path, "r") as csvfile:
            csvreader = csv.reader(csvfile)
            fields = next(csvreader)
            start_time_str = [row[1] for row in csvreader if row[0] == self.dcid][0]

        fname = "_".join(["emit" + start_time_str, str(self.frame_num).zfill(2)])
        out_path = os.path.join(data_dir, fname)
        logger.debug("Writing data to path %s" % out_path)
        with open(out_path, "wb") as f:
            f.write(self.data)


frame_path = sys.argv[1]
frame = Frame(frame_path)
frame.write_data(os.path.dirname(frame_path))
