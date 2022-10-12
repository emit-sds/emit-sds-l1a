#!/usr/bin/env python

import argparse
import datetime
import itertools

from emit_sds_l1a.ccsds_packet import ScienceDataPacket
from emit_sds_l1a.frame import Frame

PRIMARY_HDR_LEN = 6
HEADER_SYNC_WORD = bytes.fromhex("81FFFF81")

parser = argparse.ArgumentParser()
parser.add_argument("infile")
parser.add_argument("--method", type=int, default=2)
parser.add_argument("--pkt_format", default="1.3")
args = parser.parse_args()

in_file = open(args.infile, "rb")
data = bytearray()

print(datetime.datetime.now())

cnt = 0
while True:
    try:
        pkt = ScienceDataPacket(in_file, pkt_format=args.pkt_format)
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
    last_index = 0
    last_size = 0
    for i in range(len(data) - len(HEADER_SYNC_WORD)):
        if data[i: i + len(HEADER_SYNC_WORD)] == HEADER_SYNC_WORD:
            indices.append(i)
            frame = Frame(data[i: i + 1280])
            fname = "_".join([str(frame.dcid).zfill(10), frame.start_time.strftime("%Y%m%dt%H%M%S"),
                              str(frame.frame_count_in_acq).zfill(5), str(frame.planned_num_frames).zfill(5),
                              str(frame.acq_status), str(frame.processed_flag)])
            print(f"- Size since last index: {i - last_index}")
            print(f"- Correct size: {True if last_size + 1280 == i - last_index else False}")
            print(f"Index: {i}, Frame: {fname}, Valid: {frame.is_valid()}, Data Size: {frame.data_size}")
            last_index = i
            last_size = frame.data_size
            # if len(indices) > 2:
            #     break

# print(indices)
print(f"Total sync words found: {len(indices)}")
print(datetime.datetime.now())
