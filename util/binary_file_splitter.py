"""
This utility reads in a binary file and splits it in two at a certain byte index.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import os
import sys

in_path = sys.argv[1]
index = int(sys.argv[2])

file_size = os.path.getsize(in_path)

print(f"Splitting file of size {file_size} at index {index}.")

if index >= file_size:
    print("Index is too large for file.  Exiting...")

print(f"Resulting files should be {index} bytes and {file_size - index} bytes in size.")

f = open(in_path, "rb")
with open(in_path + "_s0", "wb") as out_s0:
    out_s0.write(f.read(index))
with open(in_path + "_s1", "wb") as out_s1:
    out_s1.write(f.read())
f.close()
