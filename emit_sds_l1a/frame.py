"""
The Frame class is used to read acquisition frames with uncompressed frame headers and compressed frame data.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import sys

logger = logging.getLogger("emit-sds-l1a")


class Frame:

    def __init__(self, frame_binary):
        logger.debug("Initializing frame")
        HDR_NUM_BYTES = 1280
        self.hdr = frame_binary[0: HDR_NUM_BYTES]
        self.sync_word = self.hdr[0:4]
        self.data_size = int.from_bytes(self.hdr[4:8], byteorder="little", signed=False)
        self.data = frame_binary[HDR_NUM_BYTES:]
        self.frame_count = int.from_bytes(self.hdr[8:16], byteorder="little", signed=False)
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

    def __repr__(self):
        repr = "<Frame: sync_word={} data_size={} frame_count={} compression_flag={} processed_flag={} dcid={} ".format(
            self.sync_word, self.data_size, self.frame_count, self.compression_flag, self.processed_flag, self.dcid)
        repr += "acq_status={} first_frame_flag={} cloudy_flag={} frame_count_in_acq={} solar_zenith={} ".format(
            self.acq_status, self.first_frame_flag, self.cloudy_flag, self.frame_count_in_acq, self.solar_zenith)
        repr += "planned_num_frames={}>".format(self.planned_num_frames)
        return repr

    def save(self, out_dir):
        fname = "_".join([self.dcid, str(self.frame_count).zfill(5), str(self.acq_status)])
        out_path = os.path.join(out_dir, fname)
        logger.debug("Writing frame to path %s" % out_path)
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
            fname = "_".join([dcid, str(frame.frame_count).zfill(5)])
            out_path = os.path.join(out_dir, fname)
            logger.debug("Writing frame to path %s" % out_path)
            with open(out_path, "wb") as f:
                f.write(hdr + data)
            # Write just the data to file
            data_out_path = out_path + "_data"
            frame.write_data(data_out_path)

            hdr = self.stream.read(1280)

        logger.debug("Reached EOF.")

