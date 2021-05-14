"""
The Frame class is used to read acquisition frames with uncompressed frame headers and compressed frame data.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
logger = logging.getLogger("emit-sds-l1a")


class Frame:

    def __init__(self, frame_binary):
        # TODO: Change this from frame_path to frame
        logger.debug("Initializing frame")
        HDR_NUM_BYTES = 1280
        self.hdr = frame_binary[0: HDR_NUM_BYTES]
        data_size = int.from_bytes(self.hdr[4:8], byteorder="little", signed=False)
        self.data = frame_binary[HDR_NUM_BYTES: HDR_NUM_BYTES + data_size]
        self.data_filler = frame_binary[HDR_NUM_BYTES + data_size:]
        self.dcid = self.hdr[28:32].decode("utf-8")
        self.frame_num = int.from_bytes(self.hdr[8:16], byteorder="little", signed=False)
        self.acq_status = int.from_bytes(self.hdr[32:36], byteorder="little", signed=False)

    def save(self, out_dir):
        fname = "_".join([self.dcid, str(self.frame_num).zfill(5), str(self.acq_status)])
        out_path = os.path.join(out_dir, fname)
        logger.debug("Writing frame to path %s" % out_path)
        logger.debug("data length is %s" % len(self.data))
        logger.debug(f"data filler length is {len(self.data_filler)}")
        with open(out_path, "wb") as f:
            f.write(self.hdr + self.data + self.data_filler)

    def write_data(self, out_path):
        # fname = "_".join(["emit" + start_time_str, str(self.frame_num).zfill(2)])
        # out_path = os.path.join(data_dir, fname)
        logger.debug("Writing data to path %s" % out_path)
        with open(out_path, "wb") as f:
            f.write(self.data)
