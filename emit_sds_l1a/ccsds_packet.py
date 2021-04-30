"""
This utility reads a CCSDS packet stream file and depacketizes it.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import itertools
import logging
import os
import sys

from emit_sds_l1a.frame import Frame

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
logger = logging.getLogger("emit-sds-l10")

CCSDS_PKT_SEC_COUNT_MOD = 16384
HDR_NUM_BYTES = 6
SEC_HDR_LEN = 10
MAX_PKT_DATA_FIELD = 65536
MAX_USER_DATA_FIELD = MAX_PKT_DATA_FIELD - SEC_HDR_LEN


class SciencePacketProcessingException(Exception):
    pass


class CCSDSPacket:

    def __init__(self, stream=None, **kwargs):
        logger.debug(f"Initializing CCSSDS Packet")
        if stream:
            self.read(stream)
        else:
            self.hdr_data = kwargs.get("hdr_data", bytearray(6))
            self._parse_header(self.hdr_data)
            self.body = kwargs.get("body", bytearray())

    def _parse_header(self, hdr):
        """"""
        logger.debug("primary header: " + str([bin(hdr[i])[2:].zfill(8) for i in range(HDR_NUM_BYTES)]))
        self.HEADER_SYNC_WORD = 0x81FFFF81
        self.pkt_ver_num = (hdr[0] & 0xE0) >> 5
        self.pkt_type = (hdr[0] & 0x10) >> 4
        self.sec_hdr_flag = (hdr[0] & 0x08) >> 3
        self.apid = int.from_bytes(hdr[0:2], "big") & 0x07FF
        self.seq_flags = (hdr[2] & 0xC0) >> 6
        self.pkt_seq_cnt = int.from_bytes(hdr[2:4], "big") & 0x3FFF
        self.pkt_data_len = int.from_bytes(hdr[4:6], "big")

    def read(self, stream):
        """ Read packet data from a stream
        :param stream: A file object from which to read data
        :type stream: file object
        """
        self.hdr_data = stream.read(6)
        if len(self.hdr_data) != 6:
            raise EOFError("CCSDS Header Read failed due to EOF")

        self._parse_header(self.hdr_data)
        # Packet Data Length is expressed as "number of packets in
        # packet data field minus 1"
        self.body = stream.read(self.pkt_data_len + 1)
        # self.data = self.body[SEC_HDR_LEN:]

    @property
    def is_header_packet(self):
        stat = False
        if self.data and len(self.data) >= 4:
            stat = (
                int.from_bytes(self.data[:4], byteorder="big")
                == self.HEADER_SYNC_WORD
            )
        return stat

    @property
    def product_length(self):
        length = -1
        if self.is_header_packet and len(self.data) >= 8:
            length = int.from_bytes(self.data[4:8], byteorder="little")
        else:
            logger.error(
                f"EngineeringDataPacket.product_length is returning {length}. "
                f"Is Header Pkt: {self.is_header_packet} | len: {len(self.body)}"
            )
        return length

    @property
    def data(self):
        if self.body:
            return self.body[SEC_HDR_LEN :]
        else:
            return None

    @data.setter
    def data(self, data):
        self.body = self.body[: SEC_HDR_LEN] + data

    def __repr__(self):
        return "<CCSDSPacket: pkt_ver_num={} pkt_type={} apid={} pkt_seq_cnt={} pkt_data_len={}".format(
            self.pkt_ver_num, self.pkt_type, self.apid, self.pkt_seq_cnt, self.pkt_data_len
        )


class SciencePacketProcessor:

    def __init__(self, stream_path):
        logger.debug(f"Initializing SciencePacketProcessor from path {stream_path}")
        self.HEADER_SYNC_WORD = bytes.fromhex("81FFFF81")
        self.MIN_PROCABLE_PKT_LEN = 8
        self.stream = open(stream_path, "rb")
        self._pkt_partial = None

    def read_frame(self):
        # Read a science frame from the stream
        logger.debug("Beginning science frame read")
        while True:
            try:
                # TODO: How to handle missing packets and overlap packets (already read)
                start_pkt = self._read_frame_start_packet()
                pkt_parts = self._read_pkt_parts(start_pkt)
                return self._reconstruct_frame(pkt_parts)
            except EOFError:
                logger.info(
                    "Received EOFError when reading files. No more data to process"
                )
                sys.exit()

    def _read_next_packet(self):
        pkt = CCSDSPacket(stream=self.stream)
        logger.debug(pkt)
        return pkt

    def _read_frame_start_packet(self):
        while True:
            pkt = None
            if self._pkt_partial:
                index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, self._pkt_partial.data)
                if index:
                    pkt = self._pkt_partial
                    self._pkt_partial = None
            if not pkt:
                pkt = self._read_next_packet()
                if self._pkt_partial:
                    pkt.data = self._pkt_partial.data + pkt.data
                    self._pkt_partial = None
            index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, pkt.data)
            if index is not None:
                logger.debug(f"Found sync word at index {index}")
                pkt.data = pkt.data[index:]
                # Read follow on packet if data doesn't contain enough info (SYNC WORD + frame img size)
                if len(pkt.data) < self.MIN_PROCABLE_PKT_LEN:
                    logger.debug(
                        "Located HEADER packet is too small for further processing. "
                        f"Reading and melding into next packet. Size: {len(pkt.data)}"
                    )
                    next_pkt = self._read_next_packet()
                    next_pkt.data = pkt.data + next_pkt.data
                    pkt = next_pkt

                return pkt

    def _read_pkt_parts(self, start_pkt):
        # Expected frame size is data length plus 1280 bytes for header
        frame_data_filler_len = 0
        if start_pkt.product_length % 16 > 0:
            frame_data_filler_len = 16 - start_pkt.product_length % 16
        expected_frame_len = start_pkt.product_length + 1280 + frame_data_filler_len
        logger.debug(f"Start packet says frame img size is {expected_frame_len}")
        # Handle case where frame data is less than current packet data size
        if expected_frame_len < len(start_pkt.data):
            # Check for next sync word in this segment before we read it
            index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, start_pkt.data[4:expected_frame_len])
            if index:
                msg = (
                    "Found another instance of sync word while attempting to read frame smaller than packet length."
                    f"Index found at: {index + 4}, Exp frame len: {expected_frame_len}."
                )
                raise SciencePacketProcessingException(msg)
            else:
                # Create a partial and read in short frame
                partial_body = start_pkt.data[expected_frame_len:]
                partial = CCSDSPacket(hdr_data=start_pkt.hdr_data, body=start_pkt.body[:10] + partial_body)
                self._pkt_partial = partial

                start_pkt.data = start_pkt.data[:expected_frame_len]
                pkt_parts = [start_pkt]
                return pkt_parts

        data_accum_len = len(start_pkt.data)
        logger.debug(f"Adding {len(start_pkt.data)}.  Accum data is now {data_accum_len}")
        pkt_parts = [start_pkt]

        while True:
            # TODO: What about encountering sync word too soon?  Assume we fill in missing packets for now
            if data_accum_len == expected_frame_len:
                # We're done
                logger.debug("Case 1")
                return pkt_parts
            elif expected_frame_len < data_accum_len < expected_frame_len + 4:
                # Sync word may span packets, so create partial based on expected length
                logger.debug("Case 2")
                remaining_bytes = data_accum_len - expected_frame_len
                partial_body = pkt_parts[-1].data[-remaining_bytes:]
                partial = CCSDSPacket(hdr_data=pkt_parts[-1].hdr_data, body=pkt_parts[-1].body[:10] + partial_body)
                self._pkt_partial = partial

                pkt_parts[-1].data = pkt_parts[-1].data[:-remaining_bytes]
                return pkt_parts
            elif data_accum_len >= expected_frame_len + 4:
                # Look for next sync word and throw exception if not found
                logger.debug("Case 3")
                index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, pkt_parts[-1].data)
                if index:
                    # Create partial first and then remove extra data from pkt_parts
                    partial_body = pkt_parts[-1].data[index:]
                    partial = CCSDSPacket(hdr_data=pkt_parts[-1].hdr_data, body=pkt_parts[-1].body[:10] + partial_body)
                    self._pkt_partial = partial

                    pkt_parts[-1].data = pkt_parts[-1].data[:index]
                    return pkt_parts

                else:
                    msg = (
                        "Read processed data length is > expected frame product type length "
                        f"failed. Read data len: {data_accum_len}, Exp len: {expected_frame_len}."
                    )
                    raise SciencePacketProcessingException(msg)

            pkt = self._read_next_packet()
            pkt_parts.append(pkt)
            data_accum_len += len(pkt.data)
            logger.debug(f"Adding {len(start_pkt.data)}.  Accum data is now {data_accum_len}")

    def _reconstruct_frame(self, pkt_parts):
        frame = bytearray()
        for pkt in pkt_parts:
            frame += pkt.data
        return frame

    def _locate_sync_word_index(self, sync_word, data):
        """"""
        index = None

        data_iters = itertools.tee(data, len(sync_word))
        for i, it in enumerate(data_iters):
            next(itertools.islice(it, i, i), None)

        for i, chunk in enumerate(zip(*data_iters)):
            if bytearray(chunk) == sync_word:
                index = i
                break

        return index

# TODO: Move to separate run file
def main():
    stream_path = sys.argv[1]
    processor = SciencePacketProcessor(stream_path)
    out_dir = sys.argv[2]
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    while True:
        frame_binary = processor.read_frame()
        frame = Frame(frame_binary)
        frame.save(out_dir)


if __name__ == '__main__':
    main()
