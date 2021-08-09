"""
This utility reads a binary file and writes it out to disk as a stream of CCSDS packets.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import sys
import time
import zlib

logging.basicConfig(filename='ccsds_spoofer.log', format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger("emit-sds-l10")

CCSDS_PKT_SEC_COUNT_MOD = 16384
HDR_NUM_BYTES = 6
SEC_HDR_LEN = 11
MAX_PAYLOAD_LEN = 1479
CRC_LEN = 4


class CCSDSPacketSpoofer:

    def __init__(self, psc, data):
        logger.info(f"Initializing CCSSDS Packet with PSC {psc} and data size {len(data)}")
        self.pkt_ver_num = 0
        self.pkt_type = 0
        self.sec_hdr_flag = 1
        self.apid = 1675
        self.seq_flags = 0
        self.psc = psc
        self.data = data
        self.pkt_data_len = SEC_HDR_LEN + len(data) + CRC_LEN - 1
        self.hdr = self._construct_hdr()
        self.body = self._construct_body()
        self.course_time = int.from_bytes(self.body[:4], "big")
        self.fine_time = self.body[4]

    def _construct_hdr(self):
        logger.debug("Constructing hdr")
        hdr = bytearray(HDR_NUM_BYTES)

        hdr[0] = hdr[0] | (self.pkt_ver_num << 5)
        hdr[0] = hdr[0] | (self.pkt_type << 4)
        hdr[0] = hdr[0] | (self.sec_hdr_flag << 3)
        logger.debug("primary header: " + str([bin(hdr[i])[2:].zfill(8) for i in range(HDR_NUM_BYTES)]))
        apid_bytes = self.apid.to_bytes(2, byteorder="big", signed=False)
        logger.debug("apid:           " + str([bin(apid_bytes[i])[2:].zfill(8) for i in range(2)]))
        hdr[0] = hdr[0] | apid_bytes[0]
        hdr[1] = hdr[1] | apid_bytes[1]
        logger.debug("primary header: " + str([bin(hdr[i])[2:].zfill(8) for i in range(HDR_NUM_BYTES)]))
        psc_bytes = self.psc.to_bytes(2, byteorder="big", signed=False)
        logger.debug("psc:                                    " + str([bin(psc_bytes[i])[2:].zfill(8) for i in range(2)]))
        hdr[2] = hdr[2] | psc_bytes[0]
        hdr[3] = hdr[3] | psc_bytes[1]
        logger.debug("primary header: " + str([bin(hdr[i])[2:].zfill(8) for i in range(HDR_NUM_BYTES)]))
        hdr[4:] = self.pkt_data_len.to_bytes(2, byteorder="big", signed=False)
        logger.debug("primary header: " + str([bin(hdr[i])[2:].zfill(8) for i in range(HDR_NUM_BYTES)]))
        return hdr

    def _construct_body(self):
        logger.debug("Constructing body")
        sec_hdr = bytearray(SEC_HDR_LEN)
        # Add course and fine time
        course_time = int(time.time())
        fine_time = int((time.time() % 1) * 256)
        sec_hdr[:4] = course_time.to_bytes(4, byteorder="big", signed=False)
        sec_hdr[4] = fine_time
        calc_crc = zlib.crc32(self.data)
        crc = calc_crc.to_bytes(CRC_LEN, byteorder="big", signed=False)
        return sec_hdr + self.data + crc

    def get_packet_bytes(self):
        logger.debug("Returning packet as bytes")
        return self.hdr + self.body

    def __repr__(self):
        pkt_str = "<CCSDSPacket: pkt_ver_num={} pkt_type={} apid={} pkt_data_len={} ".format(
            self.pkt_ver_num, self.pkt_type, self.apid, self.pkt_data_len)
        pkt_str += "course_time={} fine_time{} pkt_seq_cnt={}>".format(self.course_time, self.fine_time, self.psc)
        return pkt_str


data_path = sys.argv[1]
out_path = data_path + "_ccsds.bin"

with open(data_path, "rb") as f:
    psc = 0
    data = f.read(MAX_PAYLOAD_LEN)
    ccsds_stream = bytearray()
    while len(data) > 0:
        packet = CCSDSPacketSpoofer(psc, data)
        logger.info(packet)
        ccsds_stream += packet.get_packet_bytes()
        # Increment for next loop
        data = f.read(MAX_PAYLOAD_LEN)
        psc = (psc + 1) % CCSDS_PKT_SEC_COUNT_MOD
    with open(out_path, "wb") as out_file:
        out_file.write(ccsds_stream)
