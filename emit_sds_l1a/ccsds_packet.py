"""
This utility reads a CCSDS packet stream file and depacketizes it.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import itertools
import logging
import zlib

from enum import Enum
from sortedcontainers import SortedDict

logger = logging.getLogger("emit-sds-l1a")


class PSCMismatchException(Exception):
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
            self.seq_flags == self.SeqFlags.FIRST_SEG.value or self.seq_flags == self.SeqFlags.UNSEG.value
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
        crc = int.from_bytes(self.body[-self.CRC_LEN:], "big")
        calc_crc = zlib.crc32(self.body[self.SEC_HDR_LEN: -self.CRC_LEN])
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
            stat = (int.from_bytes(self.data[:4], byteorder="big") == self.HEADER_SYNC_WORD)
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
        pkt_str = "<CCSDSPacket: pkt_ver_num={} pkt_type={} apid={} pkt_data_len={} ".format(
            self.pkt_ver_num, self.pkt_type, self.apid, self.pkt_data_len)
        pkt_str += "course_time={} fine_time{} pkt_seq_cnt={}>".format(
            self.course_time, self.fine_time, self.pkt_seq_cnt)
        return pkt_str


class SDPProcessingStats:
    """Science Data Product Processing Stats
    A class for tracking various stats associated with processing
    Science Data Product Packets. Provides an API for marking
    encountered issues and prints in a human-readable format when
    when converted to a string.
    """

    def __init__(self):
        self._stats = {
            "ccsds_pkts_read": 0,
            "bytes_read": 0,
            "truncated_frame_errors": 0,
            "invalid_pkt_errors": 0,
            "invalid_psc": [],
            "pkt_seq_errors": 0,
            "missing_psc": []
        }

    def ccsds_read(self, pkt):
        self._stats["ccsds_pkts_read"] += 1
        self._stats["bytes_read"] += pkt.size

    def pkt_seq_err(self, current_pkt, expected_psc):
        self._stats["pkt_seq_errors"] += 1

        cur_course = current_pkt.course_time
        cur_fine = current_pkt.fine_time
        cur_psc = current_pkt.pkt_seq_cnt

        # TODO: Add padding for alphabetical sort
        if cur_psc > expected_psc:
            for i in range(expected_psc, cur_psc):
                self._stats["missing_psc"].append(f"{cur_course}_{cur_fine}_{i}")
        else:
            for i in range(expected_psc, CCSDSPacket.CCSDS_PKT_SEC_COUNT_MOD):
                self._stats["missing_psc"].append(f"{cur_course}_{cur_fine}_{i}")

            for i in range(cur_psc):
                self._stats["missing_psc"].append(f"{cur_course}_{cur_fine}_{i}")

    def invalid_pkt(self, pkt):
        self._stats["invalid_pkt_errors"] += 1
        self._stats["invalid_psc"].append(f"{pkt.course_time}_{pkt.fine_time}_{pkt.pkt_seq_cnt}")

    def truncated_frame(self):
        self._stats["truncated_frame_errors"] += 1

    def __str__(self):
        self._stats["missing_psc"].sort()
        missing_pscs_str = "\n".join([i for i in self._stats["missing_psc"]])

        self._stats["invalid_psc"].sort()
        invalid_pscs_str = "\n".join([i for i in self._stats["invalid_psc"]])

        return (
            "SDP PROCESSING STATS\n"
            "--------------------\n\n"
            f"Total CCSDS Packets Read: {self._stats['ccsds_pkts_read']}\n"
            f"Total bytes read: {self._stats['bytes_read']}\n\n"
            f"Truncated Frame Errors Encountered: {self._stats['truncated_frame_errors']}\n\n"
            f"Invalid Packet Errors Encountered: {self._stats['invalid_pkt_errors']}\n"
            "Invalid Packet Values:\n"
            f"{invalid_pscs_str}\n\n"
            f"Packet Sequence Count Errors Encountered: {self._stats['pkt_seq_errors']}\n"
            f"Total Missing Packet Sequence Count Values: {len(self._stats['missing_psc'])}\n"
            "Missing Packet Sequence Values:\n"
            f"{missing_pscs_str}\n\n"
        )


class SciencePacketProcessor:

    HEADER_SYNC_WORD = bytes.fromhex("81FFFF81")
    SEC_HDR_LEN = 11
    MIN_PROCABLE_PKT_LEN = 8
    CRC_LEN = 4

    def __init__(self, stream_path):
        logger.debug(f"Initializing SciencePacketProcessor from path {stream_path}")
        self.stream = open(stream_path, "rb")
        self._cur_psc = -1
        self._cur_coarse = -1
        self._cur_fine = -1
        self._processed_pkts = SortedDict()
        self._pkt_partial = None
        self._stats = SDPProcessingStats()

    def read_frame(self):
        # Read a science frame from the stream
        while True:
            try:
                logger.info(f"READ FRAME START")
                start_pkt = self._read_frame_start_packet()
                pkt_parts = self._read_pkt_parts(start_pkt)
                logger.info(f"READ FRAME END")
                return self._reconstruct_frame(pkt_parts)
            except EOFError:
                logger.info(
                    "Received EOFError when reading files. No more data to process"
                )
                raise EOFError

    def _read_next_packet(self):
        while True:
            pkt = ScienceDataPacket(stream=self.stream)
            self._stats.ccsds_read(pkt)
            pkt_hash = str(pkt.course_time) + str(pkt.fine_time) + str(pkt.pkt_seq_cnt)

            # Handle case where packet is not valid
            if not pkt.is_valid:
                self._stats.invalid_pkt(pkt)
                # Log the issue and skip packet, which should then result in PSC mismatch
                logger.warning(f"Skipping next packet because it is invalid: {pkt}")
                continue

            # Handle the case where this is the first packet read
            if self._cur_psc < 0:
                # Initialize self.cur_psc and return packet
                self._cur_psc = pkt.pkt_seq_cnt
                self._cur_coarse = pkt.course_time
                self._cur_fine = pkt.fine_time
                self._processed_pkts[pkt_hash] = True
                return pkt

            # Get next psc and compare time keys
            next_psc = CCSDSPacket.next_psc(self._cur_psc)
            pkt_time_key = str(pkt.course_time).zfill(10) + str(pkt.fine_time).zfill(3)
            cur_time_key = str(self._cur_coarse).zfill(10) + str(self._cur_fine).zfill(3)

            # Handle the happy case when next_psc matches the read packet and the time keys are in order.
            if pkt_time_key >= cur_time_key and pkt.pkt_seq_cnt == next_psc:
                self._cur_psc = pkt.pkt_seq_cnt
                self._cur_coarse = pkt.course_time
                self._cur_fine = pkt.fine_time
                self._processed_pkts[pkt_hash] = True
                return pkt

            # Handle the case where the packet has been seen before (this would happen in an overlap)
            if pkt_hash in self._processed_pkts:
                logger.warning(f"Next packet read with hash ({pkt_hash}) has been seen before. Skipping...")
                continue

            # If above cases fail, then this must be a PSC mismatch.
            # NOTE: This assumes that the size of the overlap is less than the size of a frame
            if pkt.pkt_seq_cnt != next_psc:
                # PSC mismatch - reset cur psc and remove any partial packet
                self._stats.pkt_seq_err(pkt, next_psc)
                self._cur_psc = pkt.pkt_seq_cnt
                self._cur_coarse = pkt.course_time
                self._cur_fine = pkt.fine_time
                self._processed_pkts[pkt_hash] = True
                self._pkt_partial = pkt
                msg = f"Expected next psc of {next_psc} not equal to the psc of the next packet read {pkt.pkt_seq_cnt}"
                raise PSCMismatchException(msg)

    def _read_frame_start_packet(self):
        while True:
            try:
                pkt = None

                # Look for index in partial packet first and read next packet if not found
                if self._pkt_partial:
                    # Look for sync word in partial packet if it exists
                    index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, self._pkt_partial.data)
                    if index is not None:
                        # If sync word found, use partial packet as start packet and reset to None.
                        # No need to read a new packet in this case.
                        pkt = self._pkt_partial
                        self._pkt_partial = None

                # If sync word not found in partial packet, read the next packet
                if not pkt:
                    pkt = self._read_next_packet()
                    if self._pkt_partial:
                        # Assign partial packet's data to beginning of pkt and reset to None.
                        # Sync word may span partial packet and next packet
                        pkt.data = self._pkt_partial.data + pkt.data
                        self._pkt_partial = None

                # Having taken care of partial packet, look for sync word again
                index = self._locate_sync_word_index(self.HEADER_SYNC_WORD, pkt.data)
                if index is not None:
                    # If sync word is found, check minimum processable length and read next packet if needed
                    logger.info(f"Found sync word at index {index} in packet {pkt}")
                    # Remove data before sync word so packet data starts at the beginning of the frame
                    pkt.data = pkt.data[index:]
                    # Read follow on packet if data doesn't contain enough info (SYNC WORD + frame img size)
                    if len(pkt.data) < self.MIN_PROCABLE_PKT_LEN:
                        logger.info(
                            "Located HEADER packet is too small for further processing. "
                            f"Reading and melding into next packet. Size: {len(pkt.data)}"
                        )
                        next_pkt = self._read_next_packet()
                        next_pkt.data = pkt.data + next_pkt.data
                        pkt = next_pkt

                    return pkt

                else:
                    # Save the last chunk of packet data equal to the length of
                    # the HEADER sync word so we can handle the sync word being
                    # split across packets.
                    logger.warning("Unable to find header sync word. Skipping packet {pkt}")
                    self._pkt_partial = pkt
                    self._pkt_partial.data = self._pkt_partial.data[-len(self.HEADER_SYNC_WORD):]

            except PSCMismatchException as e:
                logger.warning(e)
                logger.warning("While looking for frame start packet, encountered PSC mismatch.")

    def _read_pkt_parts(self, start_pkt):
        # Expected frame size is data length plus 1280 bytes for header
        expected_frame_len = start_pkt.product_length + 1280
        logger.debug(f"Start packet says frame img size is {expected_frame_len}")

        # Handle case where frame data is less than current packet data size
        if expected_frame_len < len(start_pkt.data):
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

        # If we need to read more packets, then track accumulated data length and continue reading until the
        # accumulated data equals or exceeds the expected frame length
        data_accum_len = len(start_pkt.data)
        logger.debug(f"Adding {len(start_pkt.data)}.  Accum data is now {data_accum_len}")
        pkt_parts = [start_pkt]

        while True:
            if data_accum_len == expected_frame_len:

                # We're done
                logger.debug("Case 1 - accumulated data length equals expected frame length. Returning packet parts.")
                return pkt_parts

            elif data_accum_len > expected_frame_len:

                # We've read enough and must trim the last packet in the pkt_parts to the expected size.
                # Also, Save the trimmed portion as a partial packet.
                logger.debug("Case 2 - accumulated data length exceeds expected length. Trimming last packet to "
                             "expected size and creating partial packet of remaining bytes.")
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

            # If neither of the above end cases is met,  then read the next packet
            try:
                pkt = self._read_next_packet()
            except PSCMismatchException as e:
                logger.warning(e)
                logger.warning("While reading packet parts, encountered PSC mismatch. Returning truncated frame.")
                self._stats.truncated_frame()
                return pkt_parts
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

    def stats(self, out_file="sdp_stats.txt"):
        """Output stats from processing data files
        Arguments:
            out_file: A file-like object to which stats will be printed.
            verbose: Flag for setting verbose output. Currently setting this True
                only affects display of "Ignored" / Default data product stats.
                (default: False)
        """
        with open(out_file, "w") as f:
            f.write(str(self._stats))
