#!/usr/bin/env python

import argparse
import datetime
import itertools

from emit_sds_l1a.ccsds_packet import ScienceDataPacket

PRIMARY_HDR_LEN = 6
HEADER_SYNC_WORD = bytes.fromhex("81FFFF81")

parser = argparse.ArgumentParser()
parser.add_argument("infile")
parser.add_argument("method", type=int, default=1)
args = parser.parse_args()

in_file = open(args.infile, "rb")
data = bytearray()

print(datetime.datetime.now())

cnt = 0
while True:
    try:
        pkt = ScienceDataPacket(in_file)
        cnt += 1
        data += pkt.data
    except EOFError:
        break

print(f"Count of packets: {cnt}")
print(datetime.datetime.now())

indices = []

if args.method == 1:
    print("Using itertools...")
    data_iters = itertools.tee(data, len(HEADER_SYNC_WORD))
    print(f"len(data_iters): {len(data_iters)}")
    for i, it in enumerate(data_iters):
        next(itertools.islice(it, i, i), None)

    for i, chunk in enumerate(zip(*data_iters)):
        if bytearray(chunk) == HEADER_SYNC_WORD:
            indices.append(i)
else:
    print("Not using itertools...")
    for i in range(len(data) - len(HEADER_SYNC_WORD)):
        if data[i: i + len(HEADER_SYNC_WORD)] == HEADER_SYNC_WORD:
            indices.append(i)

print(indices)
print(datetime.datetime.now())
