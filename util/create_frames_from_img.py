"""
A utility script to create 32 line frames from an input image in ENVI format

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os
import sys

import numpy as np

import spectral.io.envi as envi


def main():

    input_path = sys.argv[1]
    start_line = int(sys.argv[2])
    num_frames = int(sys.argv[3])

    hdr_path = input_path.replace(".img", ".hdr") if input_path.endswith(".img") else input_path + ".hdr"
    hdr = envi.read_envi_header(hdr_path)
    lines = int(hdr['lines'])
    bands = int(hdr['bands'])
    samples = int(hdr['samples'])
    img = np.memmap(input_path, shape=(lines, bands, samples), dtype=np.uint16, mode="r")

    hdr["lines"] = 32
    line = start_line
    remaining_lines = 0
    # TODO: remove second loop
    if num_frames == 0:
        num_frames = lines // 32
        remaining_lines = lines % 32
    for frame_num in range(num_frames):
        print("Working on frame number %i" % frame_num)
        frame_path = "_".join([input_path, str(start_line), str(frame_num)])
        out_file = envi.create_image(frame_path + ".hdr", hdr, ext='', force=True)
        frame = out_file.open_memmap(interleave='source', writable=True)
        frame[:, :, :] = img[line:line + 32, :, :].copy()
        del frame
        line += 32
    if remaining_lines > 0:
        hdr["lines"] = remaining_lines
        print("Working on final frame with extra %i lines" % remaining_lines)
        frame_path = "_".join([input_path, str(start_line), str(frame_num + 1)])
        out_file = envi.create_image(frame_path + ".hdr", hdr, ext='', force=True)
        frame = out_file.open_memmap(interleave='source', writable=True)
        frame[:, :, :] = img[line:line + remaining_lines, :, :].copy()
        del frame


if __name__ == '__main__':
    main()
