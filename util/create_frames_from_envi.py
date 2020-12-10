"""
A utility script to create 32 line frames from an input image in ENVI format

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import numpy as np
import sys

import spectral.io.envi as envi


def main():

    input_path = sys.argv[1]
    start_line = sys.argv[2]
    num_frames = sys.argv[3]

    img = envi.open(input_path + ".hdr", input_path)
    meta = img.metadata.copy()
    meta["lines"] = 32
    line = start_line
    for frame_num in range(num_frames):
        frame = np.array(img[line:line + 32, :, :])
        frame_path = "_".join(input_path, start_line, frame_num) + ".hdr"
        envi.save_image(frame_path, frame, metadata=meta, ext="", force=True)


if __name__ == '__main__':
    main()