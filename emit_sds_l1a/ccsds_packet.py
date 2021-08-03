"""
This utility reads a CCSDS packet stream file and depacketizes it.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import itertools
import logging
import sys
import zlib

from enum import Enum

logger = logging.getLogger("emit-sds-l1a")


class SciencePacketProcessingException(Exception):
    pass


class CCSDSPacket:
    """CCSDS Space Packet Object
    Provides an abstraction of a CCSDS Space Packet to simplify handling CCSDS
    packet data. The CCSDS Packet object will automatically read the necessary
    bytes for header and data if a stream is provided on initialization.
    """

    CCSDS_PKT_SEC_COUNT_MOD = 16384

    class SeqFlags(Enum):
        CONT_SEG = 0
        FIRST_SEG = 1
        LAST_SEG = 2
        UNSEG = 3

    def __init__(self, stream=None, **kwargs):
        """Inititialize CCSDSPacket
        :param stream: A file object from which to read data (default: None)
        :type stream: file object
        :param kwargs:
            - **hdr_data**: A bytes-like object containing 6-bytes
              of data that should be processed as a CCSDS Packet header.
            - **body**: The packet data field for the CCSDS
              packet. For consistency, this should be the length specified in
              the hdr_data per the CCSDS Packet format. However, this isn't
              enforced if these kwargs are used.
        """
        if stream:
            self.read(stream)
        else:
            d = bytearray(b"\x00\x00\x00\x00\x00\x00")
            self.hdr_data = kwargs.get("hdr_data", d)
            self._parse_header(self.hdr_data)
            self.body = kwargs.get("body", bytearray())

    @classmethod
    def next_psc(cls, cur_psc):
        if isinstance(cur_psc, CCSDSPacket):
            cur_psc = cur_psc.pkt_seq_cnt

        return (cur_psc + 1) % cls.CCSDS_PKT_SEC_COUNT_MOD

    @classmethod
    def prev_psc(cls, cur_psc):
        if isinstance(cur_psc, CCSDSPacket):
            cur_psc = cur_psc.pkt_seq_cnt

        return (cur_psc - 1) % cls.CCSDS_PKT_SEC_COUNT_MOD

    def _parse_header(self, hdr):
        """"""
        self.pkt_ver_num = (hdr[0] & 0xE0) >> 5
        self.pkt_type = (hdr[0] & 0x10) >> 4
        self.sec_hdr_flag = (hdr[0] & 0x08) >> 3
        self.apid = int.from_bytes(hdr[0:2], "big") & 0x07FF
        self.seq_flags = (hdr[2] & 0xC0) >> 6
        self.pkt_seq_cnt = int.from_bytes(hdr[2:4], "big") & 0x3FFF
        self.pkt_data_len = int.from_bytes(hdr[4:6], "big")

    def read(self, stream):
        """Read packet data from a stream
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

    @property
    def is_seq_start_pkt(self):
        """"""
        return (
            self.seq_flags == self.SeqFlags.FIRST_SEG.value
            or self.seq_flags == self.SeqFlags.UNSEG.value
        )

    @property
    def size(self):
        return len(self.hdr_data) + len(self.body)

    def __repr__(self):
        return "<CCSDSPacket: apid={} pkt_seq_cnt={} pkt_data_len={}".format(
            self.apid, self.pkt_seq_cnt, self.pkt_data_len
        )


class ScienceDataPacket(CCSDSPacket):
    """Science Data Packet
    A sub-class of CCSDSPacket to add functionality helpful when processing
    EMIT Ethernet Engineering Data Packets.
    """

    HEADER_SYNC_WORD = 0x81FFFF81
    PRIMARY_HDR_LEN = 6
    SEC_HDR_LEN = 11
    CRC_LEN = 4

    def __init__(self, stream=None, **kwargs):
        """Inititialize EngineeringDataPacket
        Arguments:
            stream - A file object from which to read data (default: None)
        Keyword Arguments:
            - **hdr_data**: A bytes-like object containing 6-bytes
              of data that should be processed as a CCSDS Packet header.
            - **body**: The packet data field for the CCSDS
              packet. For consistency, this should be the length specified in
              the hdr_data per the CCSDS Packet format. However, this isn't
              enforced if these kwargs are used.
        """
        super(ScienceDataPacket, self).__init__(stream=stream, **kwargs)
        logger.debug("SDP primary header: " + str([bin(self.hdr_data[i])[2:].zfill(8) for i in range(self.PRIMARY_HDR_LEN)]))

    @property
    def data(self):
        if self.body:
            return self.body[self.SEC_HDR_LEN: -self.CRC_LEN]
        else:
            return None

    @data.setter
    def data(self, data):
        self.body = self.body[:self.SEC_HDR_LEN] + data + self.body[-self.CRC_LEN:]

    @property
    def course_time(self):
        t = -1
        if len(self.body) >= 4:
            t = int.from_bytes(self.body[:4], "big")
        else:
            logging.error(
                f"Insufficient data length {len(self.body)} to extract course time "
                f"from EngineeringDataPacket. Returning default value: {t}"
            )

        return t

    @property
    def fine_time(self):
        t = -1
        if len(self.body) >= 5:
            t = self.body[4]
        else:
            logging.error(
                f"Insufficient data length {len(self.body)} to extract fine time "
                f"from EngineeringDataPacket. Returning default value: {t}"
            )

        return t

    @property
    def subheader_id(self):
        shid = -1
        if len(self.body) >= 11:
            shid = self.body[10]
        else:
            logging.error(
                f"Insufficient data length {len(self.body)} to extract subheader id "
                f"from EngineeringDataPacket. Returning default value: {shid}"
            )

        return shid

    @property
    def is_valid(self):
        """"""
        crc = int.from_bytes(self.body[-self.CRC_LEN :], "big")
        calc_crc = zlib.crc32(self.body[self.SEC_HDR_LEN : -self.CRC_LEN])
        return calc_crc == crc

    @property
    def payload_data(self):
        """"""
        if len(self.body) >= self.SEC_HDR_LEN + self.CRC_LEN:
            return self.body[self.SEC_HDR_LEN: -self.CRC_LEN]
        else:
            return bytearray()

    @property
    def is_header_packet(self):
        stat = False
        if self.data and len(self.data) >= 4:
            stat = (
                    int.from_bytes(self.data[:4], byteorder="big") == self.HEADER_SYNC_WORD
            )
        return stat

    @property
    def product_length(self):
        length = -1
        if self.is_header_packet and len(self.data) >= 8:
            length = int.from_bytes(self.data[4:8], byteorder="little")
        else:
            logger.error(
                f"ScienceDataPacket.product_length is returning {length}. "
                f"Is Header Pkt: {self.is_header_packet} | len: {len(self.body)}"
            )
        return length

    def __repr__(self):
        pkt_str = "<CCSDSPacket: pkt_ver_num={} pkt_type={} apid={} pkt_seq_cnt={} pkt_data_len={} ".format(
            self.pkt_ver_num, self.pkt_type, self.apid, self.pkt_seq_cnt, self.pkt_data_len)
        pkt_str += "course_time={} fine_time{}>".format(self.course_time, self.fine_time)
        return pkt_str


class SciencePacketProcessor:

    HEADER_SYNC_WORD = bytes.fromhex("81FFFF81")
    SEC_HDR_LEN = 11
    MIN_PROCABLE_PKT_LEN = 8
    CRC_LEN = 4

    def __init__(self, stream_path):
        logger.debug(f"Initializing SciencePacketProcessor from path {stream_path}")
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
        pkt = ScienceDataPacket(stream=self.stream)
        logger.debug(pkt)
        return pkt

    def _read_frame_start_packet(self):
        while True:
            pkt = None
            if self._pkt_partial:
                # Look for sync word in partial packet if it exists
                index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, self._pkt_partial.data)
                if index is not None:
                    # If sync word found, use partial packet as start packet and reset to None.
                    # No need to read a new packet in this case.
                    pkt = self._pkt_partial
                    self._pkt_partial = None
            if not pkt:
                # If sync word not found in partial packet, read the next packet
                pkt = self._read_next_packet()
                if self._pkt_partial:
                    # TODO: Check next_psc and throw away partial if mismatch
                    # Assign partial packet's data to beginning of pkt and reset to None.
                    # Sync word may span partial packet and next packet
                    pkt.data = self._pkt_partial.data + pkt.data
                    self._pkt_partial = None
            # Having taken care of partial packet, look for sync word again
            index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, pkt.data)
            if index is not None:
                logger.debug(f"Found sync word at index {index}")
                # Remove data before sync word so packet data starts at the beginning of the frame
                pkt.data = pkt.data[index:]
                # Read follow on packet if data doesn't contain enough info (SYNC WORD + frame img size)
                if len(pkt.data) < self.MIN_PROCABLE_PKT_LEN:
                    logger.debug(
                        "Located HEADER packet is too small for further processing. "
                        f"Reading and melding into next packet. Size: {len(pkt.data)}"
                    )
                    next_pkt = self._read_next_packet()
                    # TODO: Again look for PSC mismatch
                    next_pkt.data = pkt.data + next_pkt.data
                    pkt = next_pkt

                return pkt

            else:
                logger.debug(
                    (
                        "Attempting to read EDP start pack from packet stream "
                        f"but could not locate header sync word. Skipping packet {pkt}"
                    )
                )

                # Save the last chunk of packet data equal to the length of
                # the HEADER sync word so we can handle the sync word being
                # split across EngineeringDataPacket's Packets.
                self._pkt_partial = pkt
                self._pkt_partial.data = self._pkt_partial.data[-len(self.HEADER_SYNC_WORD):]

    def _read_pkt_parts(self, start_pkt):
        # Expected frame size is data length plus 1280 bytes for header
        expected_frame_len = start_pkt.product_length + 1280
        logger.debug(f"Start packet says frame img size is {expected_frame_len}")
        # Handle case where frame data is less than current packet data size
        if expected_frame_len < len(start_pkt.data):
            # TODO: Not sure I need to check for index here...?
            # Check for next sync word in this segment before we read it
            index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, start_pkt.data[4:expected_frame_len])
            if index is not None:
                msg = (
                    "Found another instance of sync word while attempting to read frame smaller than packet length."
                    f"Index found at: {index + 4}, Exp frame len: {expected_frame_len}."
                )
                raise SciencePacketProcessingException(msg)
            else:
                # Create a partial and then read in short frame
                partial_data = start_pkt.data[expected_frame_len:]
                partial = ScienceDataPacket(
                    hdr_data=start_pkt.hdr_data,
                    body=start_pkt.body[:self.SEC_HDR_LEN] + partial_data + start_pkt.body[-self.CRC_LEN:]
                )
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
                logger.debug("Case 1 - accumulated data length equals expected frame length")
                return pkt_parts
            elif expected_frame_len < data_accum_len < expected_frame_len + 4:
                # Sync word may span packets, so create partial based on expected length
                logger.debug("Case 2 - accumulated data length exceeds expected length but is less than expected"
                             "length + 4. Need to create partial packet before returning packet parts.")
                # Create new partial
                remaining_bytes = data_accum_len - expected_frame_len
                partial_data = pkt_parts[-1].data[-remaining_bytes:]
                partial = ScienceDataPacket(
                    hdr_data=pkt_parts[-1].hdr_data,
                    body=pkt_parts[-1].body[:self.SEC_HDR_LEN] + partial_data + pkt_parts[-1].body[-self.CRC_LEN:]
                )
                self._pkt_partial = partial

                # Remove extra data from last packet in packet parts
                pkt_parts[-1].data = pkt_parts[-1].data[:-remaining_bytes]
                return pkt_parts
            elif data_accum_len >= expected_frame_len + 4:
                # Look for next sync word and throw exception if not found
                logger.debug("Case 3 - accumulated data length exceeds expected length + 4. Look for next sync word.")
                index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, pkt_parts[-1].data)
                if index is not None:
                    # Create partial first and then remove extra data from pkt_parts
                    partial_data = pkt_parts[-1].data[index:]
                    partial = ScienceDataPacket(
                        hdr_data=pkt_parts[-1].hdr_data,
                        body=pkt_parts[-1].body[:self.SEC_HDR_LEN] + partial_data + pkt_parts[-1].body[-self.CRC_LEN:]
                    )
                    self._pkt_partial = partial

                    pkt_parts[-1].data = pkt_parts[-1].data[:index]
                    return pkt_parts

                else:
                    msg = (
                        "Read processed data length is > expected frame product type length "
                        f"failed. Read data len: {data_accum_len}, Exp len: {expected_frame_len}."
                    )
                    # TODO: Just log this and move on?
                    raise SciencePacketProcessingException(msg)

            pkt = self._read_next_packet()
            # TODO: Check PSC mismatch
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
