"""
The Frame class is used to read acquisition frames with uncompressed frame headers and compressed frame data.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import datetime as dt
import logging
import numpy as np
import os

from ait.core import dmc

logger = logging.getLogger("emit-sds-l1a")

NUM_32_BIT_UINTS = 4294967296

INSTRUMENT_MODES = {
    "LD": {
        "desc": "Line driver mode standard pin stripe image",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x02, 0x8c, 0x02, 0x02, 0x3f,
                       0x01, 0x02, 0x7c, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x9a, 0xfe, 0x8c, 0x8c, 0x22,
                       0xdd, 0xef])
    },
    "LDN": {
        "desc": "Line driver mode noise measurement",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x02, 0x8c, 0x02, 0x02, 0x3f,
                       0x01, 0x02, 0x7c, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x9a, 0xfe, 0x8c, 0x8c, 0x00,
                       0x00, 0xef])
    },
    "LDN_vdda": {
        "desc": "Line driver mode vi test on vdda noise measurement",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x02, 0x8c, 0x02, 0x02, 0x3f,
                       0x01, 0x02, 0x7c, 0x01, 0x02, 0x00, 0x00, 0x00, 0x0f, 0x40, 0x00, 0x00, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x9a, 0xfe, 0x8c, 0x8c, 0x00,
                       0x00, 0xef])
    },
    "cold_img": {
        "desc": "Nominal Cold FPA",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x02, 0x8c, 0x02, 0x02, 0x3f,
                       0x01, 0x02, 0x7c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x00, 0x20, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0xaa, 0xf5, 0x8c, 0x8c, 0x8c,
                       0x8c, 0xef])
    },
    "cold_img_vdda": {
        "desc": "Nominal and vi test set on vdda Cold FPA",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d, 0x00, 0x02, 0x8c, 0x02, 0x02, 0x3f,
                       0x01, 0x02, 0x7c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x41, 0x00, 0x20, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0xaa, 0xf5, 0x8c, 0x8c, 0x8c,
                       0x8c, 0xef])
    },
    "cold_img_mid": {
        "desc": "Gypsum Cold FPA",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xac, 0x00, 0x02, 0x8c, 0x02, 0x02, 0xa0,
                       0x00, 0x02, 0x7c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x00, 0x20, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0xaa, 0xf5, 0x8c, 0x8c, 0x8c,
                       0x8c, 0xef])
    },
    "cold_img_mid_vdda": {
        "desc": "Gypsum and vi test set on vdda Cold FPA",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xac, 0x00, 0x02, 0x8c, 0x02, 0x02, 0xa0,
                       0x00, 0x02, 0x7c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x41, 0x00, 0x20, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0xaa, 0xf5, 0x8c, 0x8c, 0x8c,
                       0x8c, 0xef])
    },
    "cold_img_slow": {
        "desc": "Maximum integration time Cold FPA",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x02, 0x8c, 0x02, 0x02, 0xff,
                       0x07, 0x02, 0x7c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x00, 0x20, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0xaa, 0xf5, 0x8c, 0x8c, 0x8c,
                       0x8c, 0xef])
    },
    "warm_img": {
        "desc": "Nominal Warm FPA",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x01, 0x02, 0x8c, 0x02, 0x02, 0x0e,
                       0x00, 0x02, 0x7c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x40, 0x00, 0x00, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x9a, 0xfe, 0x8c, 0x8c, 0x8c,
                       0x8c, 0xef])
    },
    "warm_img_short_integration": {
        "desc": "Minimum integration time Warm FPA",
        "roic_values":
            bytearray([0xc3, 0x34, 0x06, 0x00, 0x4d, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x45, 0x01, 0x02, 0x8c, 0x02, 0x02, 0x07,
                       0x00, 0x02, 0x7c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x9a, 0xfe, 0x8c, 0x8c, 0x8c,
                       0x8c, 0xef])
    },
    "warm_img_row0_row327_not_flight": {
        "desc": "Older version of Nominal Warm FPA used in testing",
        "roic_values":
            bytearray([0xc3, 0x34, 0x00, 0x00, 0x47, 0x01, 0x01, 0x00, 0x9f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3e, 0x01, 0x02, 0x8c, 0x02, 0x02, 0x0e,
                       0x00, 0x02, 0x7c, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x35, 0x20, 0x20, 0x20,
                       0x20, 0x30, 0x38, 0x37, 0x20, 0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x9a, 0xfe, 0x8c, 0x8c, 0x8c,
                       0x8c, 0xef])
    }
}


class Frame:

    def __init__(self, frame_binary, frame_hdr_format="1.0"):
        self.HDR_NUM_BYTES = 1280

        # Read fields from header
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
        self.line_timestamp = int.from_bytes(self.hdr[36:40], byteorder="little", signed=False)
        self.line_count = int.from_bytes(self.hdr[44:52], byteorder="little", signed=False)
        self.roic_register = self.hdr[108:174]
        self.frame_count_in_acq = int.from_bytes(self.hdr[810:818], byteorder="little", signed=False)
        self.solar_zenith = int.from_bytes(self.hdr[822:826], byteorder="little", signed=False)

        if frame_hdr_format == "1.0":
            self.planned_num_frames = int.from_bytes(self.hdr[922:926], byteorder="little", signed=False)
            self.os_time_timestamp = int.from_bytes(self.hdr[926:930], byteorder="little", signed=False)
            self.os_time = int.from_bytes(self.hdr[930:938], byteorder="little", signed=False)
            self.num_bands = int.from_bytes(self.hdr[938:942], byteorder="little", signed=False)
            self.coadd_mode = self.hdr[1010] & 0x01
        elif frame_hdr_format == "1.5":
            self.planned_num_frames = int.from_bytes(self.hdr[1002:1006], byteorder="little", signed=False)
            self.os_time_timestamp = int.from_bytes(self.hdr[1012:1016], byteorder="little", signed=False)
            self.os_time = int.from_bytes(self.hdr[1016:1024], byteorder="little", signed=False)
            self.num_bands = int.from_bytes(self.hdr[1024:1028], byteorder="little", signed=False)
            self.coadd_mode = self.hdr[1096] & 0x01

        self.frame_header_checksum = int.from_bytes(self.hdr[1276:1280], byteorder="little", signed=False)

        # Calculate some timing information
        self.os_time_in_utc = self._get_utc_time_from_gps(self.os_time)
        self.start_time_gps = self._calc_start_time_gps()
        self.start_time = self._get_utc_time_from_gps(self.start_time_gps)

        # Get the frame instrument_mode
        self.instrument_mode = self._get_instrument_mode()
        self.instrument_mode_desc = "No match" if self.instrument_mode == "no_match" else \
            INSTRUMENT_MODES[self.instrument_mode]["desc"]

        # Frame name and also corrupt name if needed
        self.name = "_".join([str(self.dcid).zfill(10), self.start_time.strftime("%Y%m%dt%H%M%S"),
                              str(self.frame_count_in_acq).zfill(5), str(self.planned_num_frames).zfill(5),
                              str(self.acq_status), str(self.processed_flag)])
        self.corrupt_name = "_".join([str(self.dcid).zfill(10), self.start_time.strftime("%Y%m%dt%H%M%S"),
                                      str(self.frame_count_in_acq).zfill(5), str(self.planned_num_frames).zfill(5),
                                      str(9), str(self.processed_flag)])

        logger.debug(f"Initialized frame: {self}")

    def __repr__(self):
        repr = "<Frame: sync_word={} data_size={} frame_count_pre={} frame_count_post={} compression_flag={} ".format(
            self.sync_word, self.data_size, self.frame_count_pre, self.frame_count_post, self.compression_flag)
        repr += "processed_flag={} dcid={} acq_status={} first_frame_flag={} cloudy_flag={} ".format(
            self.processed_flag, self.dcid, self.acq_status, self.first_frame_flag, self.cloudy_flag)
        repr += "line_timestamp={} line_count={} ".format(self.line_timestamp, self.line_count)
        repr += "frame_count_in_acq={} solar_zenith={} planned_num_frames={} os_time_timestamp={} os_time={} ".format(
            self.frame_count_in_acq, self.solar_zenith, self.planned_num_frames, self.os_time_timestamp, self.os_time)
        repr += " num_bands={} coadd_mode={} checksum={} os_time_utc={} start_time={} instrument_mode={}".format(
            self.num_bands, self.coadd_mode, self.frame_header_checksum, self.os_time_in_utc, self.start_time,
            self.instrument_mode)
        repr += " name={}>".format(
            self.name)
        return repr

    def _get_utc_time_from_gps(self, gps_time):
        # Convert gps_time in nanoseconds to a timestamp in utc
        d = dmc.GPS_Epoch + dt.timedelta(seconds=(gps_time / 10 ** 9))
        offset = dmc.LeapSeconds.get_GPS_offset_for_date(d)
        utc_time = d - dt.timedelta(seconds=offset)
        return utc_time

    def _calc_start_time_gps(self):
        line_timestamp = self.line_timestamp
        if self.line_timestamp < self.os_time_timestamp:
            line_timestamp = self.line_timestamp + NUM_32_BIT_UINTS
        # timestamp counter runs at 100,000 ticks per second.
        # convert to nanoseconds by dividing by 10^5 and then multiplying by 10^9 (or just multiply by 10^4)
        nanoseconds_offset = (line_timestamp - self.os_time_timestamp) * 10 ** 4
        return self.os_time + nanoseconds_offset

    def _get_instrument_mode(self):
        instrument_mode = None
        for mode, values in INSTRUMENT_MODES.items():
            if self.roic_register == bytes(values["roic_values"]):
                instrument_mode = mode
                break
        if instrument_mode is None:
            instrument_mode = "no_match"
        return instrument_mode

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

    def save(self, out_dir, corrupt=False):
        if corrupt:
            out_path = os.path.join(out_dir, self.corrupt_name)
        else:
            out_path = os.path.join(out_dir, self.name)

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

    def process_frames(self, out_dir, dcid, frame_hdr_format):
        hdr = self.stream.read(1280)
        while len(hdr) == 1280:
            logger.debug(f"len(hdr): {len(hdr)}")
            data_size = int.from_bytes(hdr[4:8], byteorder="little", signed=False)
            data = self.stream.read(data_size)
            frame = Frame(hdr + data, frame_hdr_format=frame_hdr_format)
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
