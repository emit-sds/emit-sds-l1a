"""
This utility reads in a frame from disk and offers some utility functions.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import sys

from emit_sds_l1a.frame import Frame

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger("emit-sds-l1a")

frame_path = sys.argv[1]
# frame_hdr_format is 1.0 for original frame header and 1.5 for enhanced cloud update
frame_hdr_format = sys.argv[2]

with open(frame_path, "rb") as f:
    frame_binary = f.read()
frame = Frame(frame_binary, frame_hdr_format=frame_hdr_format)
if len(sys.argv) > 3 and sys.argv[3] == "1":
    frame.write_data(frame_path + "_data")
print(frame)
