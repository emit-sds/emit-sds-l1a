#!/usr/bin/env python

import argparse

import emit.data_products as dp
# from emit_sds_l1a.ccsds_packet import ScienceDataPacket

PRIMARY_HDR_LEN = 6

parser = argparse.ArgumentParser()
parser.add_argument("infile")
args = parser.parse_args()

in_file = open(args.infile, "rb")

out_file = f"{args.infile}_mpsc_16383"
out = open(out_file, "wb")

cnt = 0
while True:
    try:
        pkt = dp.CCSDSPacket(in_file)
        if cnt % 16383 not in (0, 1):
            out.write(pkt.hdr_data)
            out.write(pkt.body)
        cnt += 1

    except EOFError:
        break

out.close()
