#!/usr/bin/env python

import argparse

from emit_sds_l1a.ccsds_packet import ScienceDataPacket


parser = argparse.ArgumentParser()
parser.add_argument("infile")
args = parser.parse_args()

in_file = open(args.infile, "rb")

prev_pkt = None
print_next_pkt = False
count = 0

while True:
    try:
        pkt = ScienceDataPacket(in_file)
        if print_next_pkt:
            print(f"Next pkt: coarse={pkt.coarse_time}, fine={pkt.fine_time}, psc={pkt.pkt_seq_cnt}")
            print_next_pkt = False
        if prev_pkt is not None:
            if pkt.coarse_time - prev_pkt.coarse_time < -1:
                print(f"Found out of order coarse times with difference of {pkt.coarse_time - prev_pkt.coarse_time}")
                print(f"Prev pkt: coarse={prev_pkt.coarse_time}, fine={prev_pkt.fine_time}, psc={prev_pkt.pkt_seq_cnt}")
                print(f"Curr pkt: coarse={pkt.coarse_time}, fine={pkt.fine_time}, psc={pkt.pkt_seq_cnt}")
                print_next_pkt = True
        count += 1
        prev_pkt = pkt

    except EOFError:
        break

print(f"Total packets: {count}")