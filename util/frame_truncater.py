"""
This utility takes a frame and truncates it to use with testing.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import sys

from emit_sds_l1a.frame import Frame

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
logger = logging.getLogger("emit-sds-l1a")

frame_path = sys.argv[1]
size = os.path.getsize(frame_path)
logger.debug(f"{frame_path} size is {size}")
with open(frame_path, "rb") as f:
    frame_binary = f.read(size - 1000000)
frame = Frame(frame_binary)
frame.write_data(frame_path + "_trunc")
