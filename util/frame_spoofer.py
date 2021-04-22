"""
This utility takes a compressed 32-line frame and prepends an uncompressed, spoofed frame header to create a binary
file similar to what is expected from the app FSW.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import sys

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
logger = logging.getLogger("emit-sds-l1a")

HDR_NUM_BYTES = 1280


class FrameSpoofer:

    def __init__(self, data_path=None):
        self.hdr = None
        self.data = None
        # If we have a frame_path, then load it in to the Frame object
        if data_path:
            self.hdr = self._construct_hdr(data_path)
            with open(data_path, "rb") as f:
                self.data = f.read()
                logger.debug("data length is %s" % len(self.data))
                if len(self.data) % 16 > 0:
                    self.data += bytearray(16 - len(self.data) % 16)
                logger.debug("data length is now %s" % len(self.data))

    def _construct_hdr(self, path):
        logger.debug("Constructing hdr bytearray")
        hdr = bytearray(HDR_NUM_BYTES)

        # Add sync word
        sync_word = bytes.fromhex("81FFFF81")
        logger.debug(sync_word)
        hdr[0:4] = sync_word
        logger.debug(b"hdr[0:10]: %b" % hdr[:10])

        # Add image size in bytes
        size = os.path.getsize(path)
        logger.debug("data size is %i" % size)
        hdr[4:8] = size.to_bytes(4, byteorder="little", signed=False)

        # Add frame number
        frame_num = int(os.path.basename(path).split("_")[8].replace(".flex", ""))
        logger.debug("frame_num is %i" % frame_num)
        hdr[8:16] = frame_num.to_bytes(8, byteorder="little", signed=False)

        # Get DCID from file name and write to hdr (as string for now)
        dcid_str = os.path.basename(path).split("_")[0][-4:]
        logger.debug("dcid_str: %s" % dcid_str)
        dcid_str_arr = bytearray(dcid_str, "utf-8")
        hdr[28:32] = dcid_str_arr

        # Set collection status
        if frame_num == 0:
            status = 0


        return hdr

    def save(self, out_dir):
        logger.debug("Saving frame to disk with frame header and compressed frame data")
        dcid = self.hdr[28:32].decode("utf-8")
        logger.debug("dcid is %s" % dcid)
        frame_num = int.from_bytes(self.hdr[8:16], byteorder="little", signed=False)
        fname = dcid + "_" + str(frame_num).zfill(2)
        out_path = os.path.join(out_dir, fname)
        binary = bytearray()
        binary += self.hdr
        binary += self.data
        with open(out_path, "wb") as f:
            f.write(binary)


data_path = sys.argv[1]
frame = FrameSpoofer(data_path)
frame.save(os.path.dirname(data_path))
