"""
This utility takes a compressed 32-line frame and prepends an uncompressed, spoofed frame header to create a binary
file similar to what is expected from the app FSW.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import logging
import os
import sys

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.DEBUG)
logger = logging.getLogger("emit-sds-l1a")

HDR_NUM_BYTES = 1280


class FrameSpoofer:

    def __init__(self, data_path, compression_flag, processed_flag, acq_status, num_frames):
        self.hdr = self._construct_hdr(data_path, compression_flag, processed_flag, acq_status, num_frames)
        with open(data_path, "rb") as f:
            self.data = f.read()
            logger.debug("data length is %s" % len(self.data))

    def _construct_hdr(self, path, compression_flag, processed_flag, acq_status, num_frames):
        logger.debug("Constructing hdr bytearray")
        hdr = bytearray(HDR_NUM_BYTES)

        # Add sync word
        sync_word = bytes.fromhex("81FFFF81")
        logger.debug(sync_word)
        hdr[0:4] = sync_word
        logger.debug(b"hdr[0:4]: %b" % hdr[:4])

        # Add image size in bytes
        size = os.path.getsize(path)
        logger.debug("data size is %i" % size)
        hdr[4:8] = size.to_bytes(4, byteorder="little", signed=False)

        # Add frame count
        frame_count = int(os.path.basename(path).split("_")[8].replace(".flex", ""))
        logger.debug("frame_count is %i" % frame_count)
        hdr[8:16] = frame_count.to_bytes(8, byteorder="little", signed=False)

        # Add frame params hdr[24:28]
        hdr[24] = hdr[24] | compression_flag
        logger.debug("hdr[24] after compression flag: " + str(bin(hdr[24])[2:].zfill(8)))

        hdr[24] = hdr[24] | (processed_flag << 2)
        logger.debug("hdr[24] after processed flag: " + str(bin(hdr[24])[2:].zfill(8)))

        # Get DCID from file name and write to hdr
        dcid_str = os.path.basename(path).split("_")[0][-4:]
        logger.debug("dcid_str: %s" % dcid_str)
        dcid_str_arr = bytearray(dcid_str, "utf-8")
        # hdr[28:32] = dcid_str_arr
        dcid = int(dcid_str)
        logger.debug(f"dcid: {dcid}")
        hdr[28:32] = dcid.to_bytes(4, byteorder="little", signed=False)

        # Set acquisition status
        hdr[32:36] = acq_status.to_bytes(4, byteorder="little", signed=False)
        logger.debug(f"acq_status is {acq_status}")
        logger.debug("hdr[32]: " + str(bin(hdr[32])[2:].zfill(8)))

        # Frame count in acquisition
        frame_count_in_acq = frame_count
        logger.debug("frame_count_in_acq is %i" % frame_count_in_acq)
        hdr[810:818] = frame_count_in_acq.to_bytes(8, byteorder="little", signed=False)

        # Planned number of frames
        logger.debug(f"planned number of frames is {num_frames}")
        hdr[922:926] = num_frames.to_bytes(4, byteorder="little", signed=False)

        return hdr

    def save(self, out_dir):
        logger.debug("Saving frame to disk with frame header and compressed frame data")
        dcid = int.from_bytes(self.hdr[28:32], byteorder="little", signed=False)
        logger.debug("dcid is %s" % dcid)
        frame_count = int.from_bytes(self.hdr[8:16], byteorder="little", signed=False)
        fname = str(dcid).zfill(4) + "_" + str(frame_count).zfill(2)
        out_path = os.path.join(out_dir, fname)
        binary = bytearray()
        binary += self.hdr
        binary += self.data
        with open(out_path, "wb") as f:
            f.write(binary)


data_path = sys.argv[1]
compression_flag = int(sys.argv[2])
processed_flag = int(sys.argv[3])
acq_status = int(sys.argv[4])
num_frames = int(sys.argv[5])
frame = FrameSpoofer(data_path, compression_flag, processed_flag, acq_status, num_frames)
frame.save(os.path.dirname(data_path))
