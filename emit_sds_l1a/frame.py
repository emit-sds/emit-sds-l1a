"""
The Frame class is used to read acquisition frames with uncompressed frame headers and compressed frame data.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import numpy as np
import os

logger = logging.getLogger("emit-sds-l1a")


class Frame:

    def __init__(self, frame_binary):
        self.HDR_NUM_BYTES = 1280
        self.hdr = frame_binary[0: self.HDR_NUM_BYTES]
        self.sync_word = self.hdr[0:4]
        self.data_size = int.from_bytes(self.hdr[4:8], byteorder="little", signed=False)
        self.data = frame_binary[self.HDR_NUM_BYTES:]
        self.frame_count_pre = int.from_bytes(self.hdr[8:16], byteorder="little", signed=False)
        self.frame_count_post = int.from_bytes(self.hdr[16:24], byteorder="little", signed=False)
        self.compression_flag = self.hdr[24] & 0x01
        self.processed_flag = (self.hdr[24] & 0x04) >> 2
        self.dcid = int.from_bytes(self.hdr[28:32], byteorder="little", signed=False)
        self.acq_status = int.from_bytes(self.hdr[32:36], byteorder="little", signed=False)
        self.first_frame_flag = self.hdr[32] & 0x01
        self.cloudy_flag = (self.hdr[32] & 0x04) >> 2
        self.line_count = int.from_bytes(self.hdr[44:52], byteorder="little", signed=False)
        self.frame_count_in_acq = int.from_bytes(self.hdr[810:818], byteorder="little", signed=False)
        self.solar_zenith = int.from_bytes(self.hdr[822:826], byteorder="little", signed=False)
        self.planned_num_frames = int.from_bytes(self.hdr[922:926], byteorder="little", signed=False)
        self.os_time = int.from_bytes(self.hdr[930:938], byteorder="little", signed=False)
        self.num_bands = int.from_bytes(self.hdr[938:942], byteorder="little", signed=False)
        self.coadd_mode = self.hdr[1010] & 0x01
        self.frame_header_checksum = int.from_bytes(self.hdr[1276:1280], byteorder="little", signed=False)
        logger.debug(f"Initialized frame: {self}")

    def __repr__(self):
        repr = "<Frame: sync_word={} data_size={} frame_count_pre={} frame_count_post={} compression_flag={} ".format(
            self.sync_word, self.data_size, self.frame_count_pre, self.frame_count_post, self.compression_flag)
        repr += "processed_flag={} dcid={} acq_status={} first_frame_flag={} cloudy_flag={} ".format(
            self.processed_flag, self.dcid, self.acq_status, self.first_frame_flag, self.cloudy_flag)
        repr += "frame_count_in_acq={} solar_zenith={} planned_num_frames={} os_time={} num_bands={} ".format(
            self.frame_count_in_acq, self.solar_zenith, self.planned_num_frames, self.os_time, self.num_bands)
        repr += "coadd_mode={} checksum={}>".format(self.coadd_mode, self.frame_header_checksum)
        return repr

    def _compute_hdr_checksum(self):
        # Compute sum of header fields up to offset 1276
        sum = 0
        for offset in range(0, self.HDR_NUM_BYTES - 4, 4):
            sum += int.from_bytes(self.hdr[offset: offset + 4], byteorder="little", signed=False)

        # Get twos complement of sum
        hdrsumbin = bin(~(np.uint32(sum)) + 1)[2:]
        if len(hdrsumbin) > 32:
            hdrsumbin = hdrsumbin[-32:]

        return int(hdrsumbin, 2)

    def is_valid(self):
        is_valid = self.frame_header_checksum == self._compute_hdr_checksum()
        logger.debug(f"Frame checksume: {self.frame_header_checksum}, "
                     f"Computed checksum: {self._compute_hdr_checksum()}")
        return is_valid

    def save(self, out_dir, test_mode=False, count=0):
        fname = "_".join([str(self.dcid).zfill(10), str(self.frame_count_in_acq).zfill(5),
                          str(self.planned_num_frames).zfill(5), str(self.acq_status)])

        # In test mode, add a numbered prefix
        if test_mode:
            fname = f"n{str(count).zfill(3)}_" + fname

        out_path = os.path.join(out_dir, fname)

        logger.info("Writing frame to path %s" % out_path)
        logger.debug("data length is %s" % len(self.data))
        with open(out_path, "wb") as f:
            f.write(self.hdr + self.data)

    def write_data(self, out_path):
        logger.debug("Writing data to path %s" % out_path)
        with open(out_path, "wb") as f:
            f.write(self.data)


class FrameStreamProcessor:

    def __init__(self, stream_path):
        self.stream = open(stream_path, "rb")

    def process_frames(self, out_dir, dcid):
        hdr = self.stream.read(1280)
        while len(hdr) == 1280:
            logger.debug(f"len(hdr): {len(hdr)}")
            data_size = int.from_bytes(hdr[4:8], byteorder="little", signed=False)
            data = self.stream.read(data_size)
            frame = Frame(hdr + data)
            print(frame)
            # Write frame (header + data) to file
            fname = "_".join([dcid, str(frame.frame_count_in_acq).zfill(5)])
            out_path = os.path.join(out_dir, fname)
            logger.debug("Writing frame to path %s" % out_path)
            with open(out_path, "wb") as f:
                f.write(hdr + data)
            # Write just the data to file
            data_out_path = out_path + "_data"
            frame.write_data(data_out_path)

            hdr = self.stream.read(1280)

        logger.debug("Reached EOF.")
