"""
This utility renames frame numbers for reassembly scenarios when only partial sets of frames are provided

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os
import sys

frame_path = sys.argv[1]
correction = int(sys.argv[2])
expected_num_frames = sys.argv[3].zfill(5)

toks = frame_path.split("_")
corrected_frame_num = str(int(toks[2]) + correction).zfill(5)
out_path = "_".join([toks[0], toks[1], corrected_frame_num, expected_num_frames, toks[4], toks[5]])

print(f"Renaming {frame_path} to {out_path}")
os.rename(frame_path, out_path)
