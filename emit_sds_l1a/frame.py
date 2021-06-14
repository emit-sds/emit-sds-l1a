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
        self.data = frame_binary[HDR_NUM_BYTES: HDR_NUM_BYTES + self.data_size]
        # self.data_filler = frame_binary[HDR_NUM_BYTES + self.data_size:]
        self.frame_count = int.from_bytes(self.hdr[8:16], byteorder="little", signed=False)
        self.frame_params = self.hdr[24:28]
        self.dcid = int.from_bytes(self.hdr[28:32], byteorder="little", signed=False)
        self.acq_status = int.from_bytes(self.hdr[32:36], byteorder="little", signed=False)
        self.line_count = int.from_bytes(self.hdr[44:52], byteorder="little", signed=False)
        self.frame_count_in_acq = int.from_bytes(self.hdr[810:818], byteorder="little", signed=False)
        self.solar_zenith = int.from_bytes(self.hdr[822:826], byteorder="little", signed=False)

        self._parse_frame_params()

    def __repr__(self):
        repr = "<Frame: sync_word={} data_size={} frame_count={} dcid={} acq_status={} line_count={} ".format(
            self.sync_word, self.data_size, self.frame_count, self.dcid, self.acq_status, self.line_count)
        repr += "frame_count_in_acq={} solar_zenith={}>".format(self.frame_count_in_acq, self.solar_zenith)
        return repr

    def _parse_frame_params(self):
        # TODO: Get compression info and cloudy info
        pass

    def save(self, out_dir):
        fname = "_".join([self.dcid, str(self.frame_count).zfill(5), str(self.acq_status)])
        out_path = os.path.join(out_dir, fname)
        logger.debug("Writing frame to path %s" % out_path)
        logger.debug("data length is %s" % len(self.data))
        # logger.debug(f"data filler length is {len(self.data_filler)}")
        with open(out_path, "wb") as f:
            f.write(self.hdr + self.data)

    def write_data(self, out_path):
        # fname = "_".join(["emit" + start_time_str, str(self.frame_count).zfill(2)])
        # out_path = os.path.join(data_dir, fname)
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
            # TODO: Find out if this is being used
            # Add data filler size if necessary
            # if data_size % 16 > 0:
            #     data_size += 16 - (data_size % 16)
            data = self.stream.read(data_size)
            frame = Frame(hdr + data)
            print(frame)
            fname = "_".join([dcid, str(frame.frame_count).zfill(5)])
            out_path = os.path.join(out_dir, fname)
            logger.debug("Writing frame to path %s" % out_path)
            with open(out_path, "wb") as f:
                f.write(hdr + data)
            data_out_path = out_path + "_data"
            frame.write_data(data_out_path)

            hdr = self.stream.read(1280)
        logger.debug("Reached EOF.")
