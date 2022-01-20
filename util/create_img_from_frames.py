"""
A utility script to combine frames into a single image

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import glob
import os
import sys

import numpy as np
import spectral.io.envi as envi


def main():

    input_dir = sys.argv[1]
    img_prefix = sys.argv[2]

    frame_paths = glob.glob(os.path.join(input_dir, img_prefix + "*flex.decomp"))
    frame_paths.sort()
    print(frame_paths)

    hdr = envi.read_envi_header(frame_paths[0] + ".hdr")
    hdr["lines"] = 32 * len(frame_paths)

    output_path = os.path.join(input_dir, img_prefix + "_combined")
    out_file = envi.create_image(output_path + ".hdr", hdr, ext='', force=True)
    output = out_file.open_memmap(interleave='source', writable=True)

    line = 0
    for path in frame_paths:
        frame = np.memmap(path, shape=(32, int(hdr["bands"]), int(hdr["samples"])), dtype=np.int16, mode="r")
        output[line:line + 32, :, :] = frame[:, :, :].copy()
        line += 32
    del output


if __name__ == '__main__':
    main()
