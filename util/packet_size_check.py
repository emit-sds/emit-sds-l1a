#!/usr/bin/env python

import argparse

import emit.data_products as dp

PRIMARY_HDR_LEN = 6

parser = argparse.ArgumentParser()
parser.add_argument("infile")
parser.add_argument("threshold", type=int)
args = parser.parse_args()

in_file = open(args.infile, "rb")

cnt = 0
skip = 4000
while True:
    try:
        pkt = dp.CCSDSPacket(in_file)
        pkt_size = PRIMARY_HDR_LEN + pkt.pkt_data_len + 1
        if pkt_size < args.threshold:
            cnt += 1
        if pkt.pkt_seq_cnt % skip == 0:
            print(f"Packet {str(pkt.pkt_seq_cnt).zfill(5)} size: {pkt_size}")
    except EOFError:
        break

print(f"Count of packets less than {args.threshold} bytes: {cnt}")
