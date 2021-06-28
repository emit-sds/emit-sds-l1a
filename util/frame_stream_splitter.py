"""
This utility reads in a frame from disk and offers some utility functions.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import sys

from emit_sds_l1a.frame import Frame, FrameStreamProcessor

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
logger = logging.getLogger("emit-sds-l1a")

stream_path = sys.argv[1]
out_dir = sys.argv[2]
dcid = sys.argv[3]

if not os.path.exists(out_dir):
    os.makedirs(out_dir)

processor = FrameStreamProcessor(stream_path)
processor.process_frames(out_dir, dcid)
