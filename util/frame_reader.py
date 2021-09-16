"""
This utility reads in a frame from disk and offers some utility functions.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import sys

from emit_sds_l1a.frame import Frame, FrameStreamProcessor

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger("emit-sds-l1a")

frame_path = sys.argv[1]
with open(frame_path, "rb") as f:
    frame_binary = f.read()
frame = Frame(frame_binary)
if len(sys.argv) > 2 and sys.argv[2] == "1":
    frame.write_data(frame_path + "_data")
print(frame)
