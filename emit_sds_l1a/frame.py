"""
The Frame class is used to read acquisition frames with uncompressed frame headers and compressed frame data.  This
class can also be used to create artificial frames from compressed data by adding frame headers.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import sys

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)


class Frame:

    def __init__(self, frame_path=None):
        hdr = None
        data = None
        # If we have a frame_path, then load it in to the Frame object
        if frame_path:
            hdr = self._construct_hdr(frame_path)
            with open(frame_path, "rb") as f:
                data = f.read()

    def _construct_hdr(self, path):
        logging.debug("Constructing hdr object")
        hdr = bytearray()
        sync_word = bytes.fromhex("81FFFF81")
        logging.debug(sync_word)
        hdr += sync_word
        return hdr

myframe = Frame(sys.argv[1])
