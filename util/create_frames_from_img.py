"""
A utility script to create 32 line frames from an input image in ENVI format

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import numpy as np
import sys

import spectral.io.envi as envi


def main():

    input_path = sys.argv[1]
    start_line = int(sys.argv[2])
    num_frames = int(sys.argv[3])

    hdr = envi.read_envi_header(input_path + ".hdr")
    lines = int(hdr['lines'])
    bands = int(hdr['bands'])
    samples = int(hdr['samples'])
    img = np.memmap(input_path, shape=(lines, bands, samples), dtype=np.uint16, mode="r")

    hdr["lines"] = 32
    line = start_line
    for frame_num in range(num_frames):
        print("Working on frame number %i" % frame_num)
        frame = np.array(img[line:line + 32, :, :])
        frame_path = "_".join([input_path, str(start_line), str(frame_num)])
        envi.save_image(frame_path + ".hdr", frame, interleave="bil", ext="", force=True)
        line += 32


if __name__ == '__main__':
    main()
