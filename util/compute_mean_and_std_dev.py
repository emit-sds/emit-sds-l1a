"""
A utility script to read in decompressed frames and compute means and std dev of means

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import sys

import numpy as np


def main():

    input_list = sys.argv[1]
    print(f"sys.byteorder: {sys.byteorder}")
    print(f"Using input list {input_list}")

    with open(input_list, "r") as f:
        paths = [line.rstrip("\n") for line in f.readlines()]

    frame_means = []
    for p in paths:
        frame = np.memmap(p, shape=(32, 328, 1280), dtype=np.uint16, mode="r")
        frame_mean = np.mean(frame[:, 1:, :])
        print(f"Mean of frame {p}: {frame_mean:.4f}")
        frame_means.append(frame_mean)
        print(frame[0, 1, 0:10])
        sample = frame[0, 1, 0]
        print(f"sample: {sample}")
        xor_sample = sample ^ 0b1000000000000000
        print(f"xor_sample: {xor_sample}")
    print(f"List of frame means: {['%.4f' % m for m in frame_means ]}")
    print(f"Std dev of frame means: {np.std(frame_means):.4f}")


if __name__ == '__main__':
    main()
