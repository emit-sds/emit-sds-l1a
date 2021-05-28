"""
This utility reads a binary file and writes it out to disk as a stream of CCSDS packets.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import sys

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
logger = logging.getLogger("emit-sds-l10")

CCSDS_PKT_SEC_COUNT_MOD = 16384
HDR_NUM_BYTES = 6
SEC_HDR_LEN = 10
MAX_PKT_DATA_FIELD = 65536
MAX_USER_DATA_FIELD = MAX_PKT_DATA_FIELD - SEC_HDR_LEN


class CCSDSPacketSpoofer:

    def __init__(self, psc, data):
        logger.debug(f"Initializing CCSSDS Packet with PSC {psc} and data size {len(data)}")
        self.pkt_ver_num = 0
        self.pkt_type = 0
        self.sec_hdr_flag = 1
        self.apid = 1675
        self.seq_flags = 0
        self.psc = psc
        self.data = data
        self.pkt_data_len = SEC_HDR_LEN + len(data) - 1
        self.hdr = self._construct_hdr()
        self.body = self._construct_body()

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
        return sec_hdr + self.data

    def get_packet_bytes(self):
        logger.debug("Returning packet as bytes")
        return self.hdr + self.body

    def save(self, out_dir):
        logger.debug("Saving frame to disk with frame header and compressed frame data")
        dcid = self.hdr[28:32].decode("utf-8")
        logger.debug("dcid is %s" % dcid)
        frame_num = int.from_bytes(self.hdr[8:16], byteorder="little", signed=False)
        fname = dcid + "_" + str(frame_num).zfill(2)
        out_path = os.path.join(out_dir, fname)
        binary = bytearray()
        binary += self.hdr
        binary += self.data
        with open(out_path, "wb") as f:
            f.write(binary)


data_path = sys.argv[1]
out_path = data_path + "_ccsds.bin"
# out_path = os.path.join(os.path.dirname(data_path), "ccsds_stream.bin")
with open(data_path, "rb") as f:
    psc = 1
    data = f.read(MAX_USER_DATA_FIELD)
    ccsds_stream = bytearray()
    while len(data) > 0:
        packet = CCSDSPacketSpoofer(psc, data)
        ccsds_stream += packet.get_packet_bytes()
        # Increment for next loop
        data = f.read(MAX_USER_DATA_FIELD)
        psc += 1
    with open(out_path, "wb") as out_file:
        out_file.write(ccsds_stream)
