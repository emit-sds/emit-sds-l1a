#!/usr/bin/env python

import argparse

import emit.data_products as dp
from emit_sds_l1a.ccsds_packet import ScienceDataPacket

PRIMARY_HDR_LEN = 6

parser = argparse.ArgumentParser()
parser.add_argument("infile")
parser.add_argument("threshold", type=int)
args = parser.parse_args()

in_file = open(args.infile, "rb")

seq_flag_counts = {
    "0": 0,
    "1": 0,
    "2": 0,
    "3": 0
}

cnt = 0
skip = 4000
total = 0
counts = {}
while True:
    try:
        pkt = ScienceDataPacket(in_file)
        total += 1
        seq_flag_counts[str(pkt.seq_flags)] += 1
        pkt_size = PRIMARY_HDR_LEN + pkt.pkt_data_len + 1
        data_size = len(pkt.data)
        if data_size in counts:
            counts[data_size] += 1
        else:
            counts[data_size] = 1
        if pkt_size < args.threshold:
            cnt += 1
        if pkt.pkt_seq_cnt % skip == 0:
            print(f"Packet {str(pkt.pkt_seq_cnt).zfill(5)} size: {pkt_size}")
    except EOFError:
        break

print(f"Total packets: {total}")
print(f"Count of packets less than {args.threshold} bytes: {cnt}")
print(f"seq_flag_counts: {seq_flag_counts}")
print(counts)
