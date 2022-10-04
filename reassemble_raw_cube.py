"""
This script reassembles the raw image cube from acquisition frames with uncompressed frame headers and compressed
frame data.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime as dt
import glob
import logging
import os
import shutil
import subprocess

from ait.core import dmc
from argparse import RawTextHelpFormatter
from collections import OrderedDict

import numpy as np
import spectral.io.envi as envi

from emit_sds_l1a.frame import Frame

NUM_32_BIT_UINTS = 4294967296
NODATA_VALUE = -9999
MISSING_FRAME_FLAG = -9998
CORRUPT_FRAME_FLAG = -9997
CORRUPT_LINE_FLAG = -9996
CLOUDY_FRAME_FLAG = -9990

INSTRUMENT_MODE_DESCRIPTIONS = {
    "LD": "Line driver mode standard pin stripe image",
    "LDN": "Line driver mode noise measurement",
    "LDN_vdda": "Line driver mode vi test on vdda noise measurement",
    "cold_img": "Nominal Cold FPA",
    "cold_img_vdda": "Nominal and vi test set on vdda Cold FPA",
    "cold_img_mid": "Gypsum Cold FPA",
    "cold_img_mid_vdda": "Gypsum and vi test set on vdda Cold FPA",
    "cold_img_slow": "Maximum integration time Cold FPA",
    "warm_img": "Nominal Warm FPA",
    "warm_img_short_integration": "Minimum integration time Warm FPA",
    "warm_img_row0_row327_not_flight": "Older version of Nominal Warm FPA used in testing",
    "no_match": "No match"
}


def get_utc_time_from_gps(gps_time):
    # Convert gps_time in nanoseconds to a timestamp in utc
    d = dmc.GPS_Epoch + dt.timedelta(seconds=(gps_time / 10 ** 9))
    offset = dmc.LeapSeconds.get_GPS_offset_for_date(d)
    utc_time = d - dt.timedelta(seconds=offset)
    return utc_time


def calculate_start_stop_times(start_times_gps):
    # Populate x, y from available gps times
    x = []
    y = []
    for i, val in enumerate(start_times_gps):
        if val is not None:
            x.append(i)
            y.append(val)
    x = np.array(x)
    y = np.array(y)
    m, b = np.polyfit(x, y, 1)

    # Create output list and fill in with start and stop times
    start_stop_times = []
    for i, val in enumerate(start_times_gps):
        if val is not None:
            start_time = get_utc_time_from_gps(val)
            stop_time = get_utc_time_from_gps(val + m)
        else:
            fit_val = m * i + b
            start_time = get_utc_time_from_gps(fit_val)
            stop_time = get_utc_time_from_gps(fit_val + m)
        start_stop_times.append([start_time, stop_time])
    return start_stop_times


def generate_line_count_lookup(line_headers, num_lines, increment, frame_num_str, start_index, stop_index, logger):
    # Return the line count lookup based on line headers and frame num
    # First, try to find two "good" lines with correct increment.
    prev_line_count = None
    good_index = None
    good_line_count = None
    for i in range(num_lines):
        line_hdr = line_headers[i, :]
        line_hdr_bytes = bytearray(line_hdr)
        line_count = int.from_bytes(line_hdr_bytes[4:8], byteorder="little", signed=False)
        if prev_line_count is not None and line_count - prev_line_count == increment:
            good_index = (int(frame_num_str) - start_index) * num_lines + i
            good_line_count = line_count
            break
        prev_line_count = line_count

    if good_index is None:
        return None

    # Now populate the line count lookups
    num_lines_in_acq = (stop_index - start_index + 1) * num_lines
    lc_lookup = [None] * num_lines_in_acq
    for i in range(num_lines_in_acq):
        lc_lookup[i] = (((i - good_index) * increment) + good_line_count) % NUM_32_BIT_UINTS

    return lc_lookup


def interpolate_missing_gps_times(lt_rows):
    # Populate x, y from available gps times
    x = []
    y = []
    for row in lt_rows:
        if int(row[1]) >= 0:
            x.append(int(row[0]))
            y.append(int(row[1]))
    x = np.array(x)
    y = np.array(y)
    m, b = np.polyfit(x, y, 1)

    for row in lt_rows:
        if int(row[1]) == -1:
            row[1] = str(int(m * int(row[0]) + b)).zfill(19)

    return lt_rows


def calculate_nanoseconds_since_gps_epoch(line_timestamp, os_time_timestamp, os_time):
    # Need to adjust line timestamp in the case where the clock rolls over (which happens about every 12 hours)
    if line_timestamp < os_time_timestamp:
        line_timestamp = line_timestamp + NUM_32_BIT_UINTS
    # timestamp counter runs at 100,000 ticks per second.
    # convert to nanoseconds by dividing by 10^5 and then multiplying by 10^9 (or just multiply by 10^4)
    line_offset_nanoseconds = (line_timestamp - os_time_timestamp) * 10 ** 4
    # OS time is in nanoseconds since GPS epoch
    return os_time + line_offset_nanoseconds


def get_utc_time_from_gps(gps_time):
    # Convert gps_time in nanoseconds to a timestamp in utc
    d = dmc.GPS_Epoch + dt.timedelta(seconds=(gps_time / 10 ** 9))
    offset = dmc.LeapSeconds.get_GPS_offset_for_date(d)
    utc_time = d - dt.timedelta(seconds=offset)
    return utc_time


def reassemble_acquisition(acq_data_paths, start_index, stop_index, start_time, stop_time, timing_info, processed_flag,
                           compression_flag, coadd_mode, num_bands, num_lines, image_dir, report_text,
                           corrupt_frames_list, failed_decompression_list, uncompressed_list, missing_frame_nums,
                           logger):
    # Reassemble frames into ENVI image cube filling in missing and cloudy data with data flags
    # First create acquisition_id from frame start_time
    # Assume acquisitions are at least 1 second long
    acquisition_id = "emit" + start_time.strftime("%Y%m%dt%H%M%S")

    hdr_path = os.path.join(image_dir, acquisition_id + "_raw.hdr")
    line_timestamps_path = hdr_path.replace("raw.hdr", "line_timestamps.txt")
    num_lines_in_acq = num_lines * len(acq_data_paths)
    hdr = {
        "description": "EMIT L1A raw instrument data (units: DN)",
        "samples": 1280,
        "lines": num_lines_in_acq,
        "bands": num_bands,
        "header offset": 0,
        "file type": "ENVI",
        "data type": 2,
        "interleave": "bil",
        "byte order": 0
    }

    envi.write_envi_header(hdr_path, hdr)
    out_file = envi.create_image(hdr_path, hdr, ext="img", force=True)
    output = out_file.open_memmap(interleave="source", writable=True)
    output[:, :, :] = NODATA_VALUE

    logger.info(f"Assembling frames into raw file with header {hdr_path}")
    logger.debug(f"Start time: {start_time}, Stop time: {stop_time}")

    cloudy_frame_nums = []
    line = 0
    lt_rows = []
    lc_increment = 2 if processed_flag == 1 and coadd_mode == 1 else 1
    lc_lookup = None
    corrupt_lines = []
    frame_corrupt_line_map = OrderedDict()
    num_valid_lines = 0
    for path in acq_data_paths:
        frame_num_str = os.path.basename(path).split(".")[0].split("_")[2]
        status = int(os.path.basename(path).split(".")[0].split("_")[4])
        start_line_in_frame = (int(frame_num_str) - start_index) * num_lines
        logger.info(f"Adding frame {path}")
        frame_corrupt_line_map[os.path.basename(path)] = []
        # If the data is compressed and not cloudy, OR if if the data is uncompressed and not cloudy or corrupt
        if (compression_flag == 1 and status in (0, 1)) or (compression_flag == 0 and status in (0, 1, 9)):
            num_valid_lines += num_lines
            # Write frame to output array
            frame = np.memmap(path, shape=(num_lines, int(hdr["bands"]), int(hdr["samples"])), dtype=np.int16, mode="r")
            output[line:line + num_lines, :, :] = frame[:, :, :].copy()
            # If the data is from the processed partition, then add 8192 to all values except the 0th band so that raw
            # DN values are in the range from 0 to 16384
            if processed_flag == 1:
                output[line:line + num_lines, 1:, :] = output[line:line + num_lines, 1:, :] + 8192
            else:
                output[line:line + num_lines, 1:, :] = np.int16((np.int64(output[line:line + num_lines, 1:, :]) + 32768) >> 2)

            # Read line headers and process below
            line_headers = frame[:, 0, :]

            # Populate line count lookup if not yet populated
            if lc_lookup is None:
                lc_lookup = generate_line_count_lookup(line_headers, num_lines, lc_increment, frame_num_str,
                                                       start_index, stop_index, logger)
                # If lc_lookup is still unpopulated it means the entire frame had corrupt lines
                if lc_lookup is None:
                    # This seems very unlikely as it would mean that all or most of the line counts were corrupt
                    logger.warning(f"Could not find incremental line counts in frame number {frame_num_str}. "
                                   f"Assuming that all lines are corrupt.")
                    corrupt_lines += list(range(start_line_in_frame, start_line_in_frame + num_lines))
                    num_valid_lines -= num_lines
                    frame_corrupt_line_map[os.path.basename(path)] = list(range(num_lines))
                    # Set all values in these lines to corrupt flag value
                    output[line:line + num_lines, 1:, :] = CORRUPT_LINE_FLAG
                else:
                    logger.info(f"Found a good line count in frame {frame_num_str} and generated a line count lookup.")

            # Loop through lines and print out line timestamps and flag corrupt lines
            for i in range(num_lines):
                # Get line header and convert to byte array
                line_hdr = line_headers[i, :]
                line_hdr_bytes = bytearray(line_hdr)
                line_timestamp = int.from_bytes(line_hdr_bytes[0:4], byteorder="little", signed=False)
                line_count = int.from_bytes(line_hdr_bytes[4:8], byteorder="little", signed=False)

                # Print out line timestamps
                nanosecs_since_gps = calculate_nanoseconds_since_gps_epoch(
                    line_timestamp=line_timestamp,
                    os_time_timestamp=timing_info[int(frame_num_str)]['os_time_timestamp'],
                    os_time=timing_info[int(frame_num_str)]['os_time']
                )
                utc_time_str = get_utc_time_from_gps(nanosecs_since_gps).strftime("%Y-%m-%dT%H:%M:%S.%f")

                # Write to timestamps file, but must insert -1 gps time for corrupt lines
                if lc_lookup is None:
                    # This line is corrupt since all lines in frame are corrupt in this case
                    lt_rows.append([str(start_line_in_frame + i).zfill(6), str(-1).zfill(19), utc_time_str,
                                    str(line_timestamp).zfill(10), str(line_count).zfill(10)])

                if lc_lookup is not None and lc_lookup[start_line_in_frame + i] != line_count:
                    logger.warning(f"Found corrupt line header at line number {start_line_in_frame + i}")
                    corrupt_line_num = start_line_in_frame + i
                    prev_corrupt_line_num = corrupt_line_num - 1
                    if prev_corrupt_line_num >= 0 and prev_corrupt_line_num not in corrupt_lines:
                        logger.warning(f"Setting previous line at index {prev_corrupt_line_num} as corrupt")
                        corrupt_lines.append(prev_corrupt_line_num)
                        num_valid_lines -= 1
                        frame_corrupt_line_map[os.path.basename(path)].append(i - 1)
                        output[prev_corrupt_line_num, 1:, :] = CORRUPT_LINE_FLAG
                    if corrupt_line_num not in corrupt_lines:
                        logger.warning(f"Setting line at index {corrupt_line_num} as corrupt")
                        corrupt_lines.append(corrupt_line_num)
                        num_valid_lines -= 1
                        frame_corrupt_line_map[os.path.basename(path)].append(i)
                        output[corrupt_line_num, 1:, :] = CORRUPT_LINE_FLAG
                    # Since this line header is corrupt, use -1 for gps time
                    lt_rows.append([str(start_line_in_frame + i).zfill(6), str(-1).zfill(19), utc_time_str,
                                    str(line_timestamp).zfill(10), str(line_count).zfill(10)])

                else:
                    # Not corrupt, so write all the values
                    lt_rows.append([str(start_line_in_frame + i).zfill(6), str(nanosecs_since_gps).zfill(19),
                                    utc_time_str, str(line_timestamp).zfill(10), str(line_count).zfill(10)])

        # Cloudy frames
        if status in (4, 5):
            cloudy_frame_nums.append(frame_num_str)
            frame = np.full(shape=(num_lines, int(hdr["bands"]), int(hdr["samples"])), fill_value=CLOUDY_FRAME_FLAG,
                            dtype=np.int16)
            output[line:line + num_lines, :, :] = frame[:, :, :].copy()
            for i in range(num_lines):
                lt_rows.append([str(start_line_in_frame + i).zfill(6), str(-1).zfill(19), "0000-00-00T00:00:00.000000",
                                str(-1).zfill(10), str(-1).zfill(10)])
        # Missing frames
        if status == 6:
            frame = np.full(shape=(num_lines, int(hdr["bands"]), int(hdr["samples"])), fill_value=MISSING_FRAME_FLAG,
                            dtype=np.int16)
            output[line:line + num_lines, :, :] = frame[:, :, :].copy()
            for i in range(num_lines):
                lt_rows.append([str(start_line_in_frame + i).zfill(6), str(-1).zfill(19), "0000-00-00T00:00:00.000000",
                                str(-1).zfill(10), str(-1).zfill(10)])
        # Failed decompression
        if status == 7:
            frame = np.full(shape=(num_lines, int(hdr["bands"]), int(hdr["samples"])),
                            fill_value=CORRUPT_FRAME_FLAG, dtype=np.int16)
            output[line:line + num_lines, :, :] = frame[:, :, :].copy()
            for i in range(num_lines):
                lt_rows.append([str(start_line_in_frame + i).zfill(6), str(-1).zfill(19), "0000-00-00T00:00:00.000000",
                                str(-1).zfill(10), str(-1).zfill(10)])

        # Corrupt and compressed
        if compression_flag == 1 and status == 9:
            frame = np.full(shape=(num_lines, int(hdr["bands"]), int(hdr["samples"])),
                            fill_value=CORRUPT_FRAME_FLAG, dtype=np.int16)
            output[line:line + num_lines, :, :] = frame[:, :, :].copy()
            for i in range(num_lines):
                lt_rows.append(
                    [str(start_line_in_frame + i).zfill(6), str(-1).zfill(19), "0000-00-00T00:00:00.000000",
                     str(-1).zfill(10), str(-1).zfill(10)])

        line += num_lines
    del output

    # Generate gpstime_lookup and replace -1 gps times with interpolated values
    if num_valid_lines >= 2:
        lt_rows = interpolate_missing_gps_times(lt_rows)
    lt_file = open(line_timestamps_path, "w")
    for row in lt_rows:
        lt_file.write(" ".join(row) + "\n")
    lt_file.close()

    # Create a reassembly report
    report_path = hdr_path.replace("_raw.hdr", "_report.txt")
    with open(report_path, "w") as f:
        f.write(report_text)

        f.write("ACQUISITION STATS\n")
        f.write("-----------------\n\n")
        f.write(f"Acquisition ID:  {acquisition_id}\n")
        f.write(f'Start time: {start_time}\n')
        f.write(f'Stop time: {stop_time}\n')
        f.write(f"Number of samples: 1280\n")
        f.write(f"Number of bands: {num_bands}\n")
        f.write(f"Number of lines: {num_lines_in_acq}\n\n")

        f.write(f"First frame number in acquisition: {str(start_index).zfill(5)}\n")
        f.write(f"Last frame number in acquisition: {str(stop_index).zfill(5)}\n\n")

        # Report on number of valid (not cloudy, missing, or corrupt) lines.
        f.write(f"Number of lines with valid data (not cloudy, missing, or corrupt): {num_valid_lines}\n\n")

        # Get timing info using loop in case the timing info is missing on the first frame.
        timing_info_found = False
        for i in range(start_index, stop_index + 1):
            if timing_info[i]['line_timestamp'] != -1:
                f.write(f"Line timestamp of first available frame ({str(i).zfill(5)}) in acquisition: "
                        f"{timing_info[i]['line_timestamp']}\n")
                f.write(f"OS time timestamp of first available frame ({str(i).zfill(5)}) in acquisition: "
                        f"{timing_info[i]['os_time_timestamp']}\n")
                f.write(f"OS time of first available frame ({str(i).zfill(5)}) in acquisition: "
                        f"{timing_info[i]['os_time']}\n\n")
                timing_info_found = True
                break
        if not timing_info_found:
            f.write(f"Line timestamp of first available frame in acquisition: -1\n")
            f.write(f"OS time timestamp of first available frame in acquisition: -1\n")
            f.write(f"OS time of first available frame in acquisition: -1\n\n")

        # Get list of acquisition frame nums and convert to padded strings like other lists
        acquisition_frame_nums = list(range(start_index, stop_index + 1))
        acquisition_frame_nums = [str(num).zfill(5) for num in acquisition_frame_nums]

        # Report on uncompressed frames
        uncompressed_in_acq = list(set(acquisition_frame_nums) & set(uncompressed_list))
        uncompressed_in_acq.sort()

        f.write(f"Total number of frames not requiring decompression in this acquisition "
                f"(compression flag set to 0 or cloudy flag set to 1): {len(uncompressed_in_acq)}\n")
        f.write("List of frame numbers not requiring decompression (if any):\n")
        if len(uncompressed_in_acq) > 0:
            f.write("\n".join(i for i in uncompressed_in_acq) + "\n")
        f.write("\n")

        # Report on corrupt frames
        corrupt_frames_in_acq = list(set(acquisition_frame_nums) & set(corrupt_frames_list))
        corrupt_frames_in_acq.sort()

        f.write(f"Total corrupt frames encountered in this acquisition: {len(corrupt_frames_in_acq)}\n")
        f.write("List of corrupt frame numbers (if any):\n")
        if len(corrupt_frames_in_acq) > 0:
            f.write("\n".join(i for i in corrupt_frames_in_acq) + "\n")
        f.write("\n")

        # Report on frames that failed decompression
        failed_decompression_in_acq = list(set(acquisition_frame_nums) & set(failed_decompression_list))
        failed_decompression_in_acq.sort()

        f.write(f"Total decompression errors encountered in this acquisition: {len(failed_decompression_in_acq)}\n")
        f.write("List of frame numbers that failed decompression (if any):\n")
        if len(failed_decompression_in_acq) > 0:
            f.write("\n".join(i for i in failed_decompression_in_acq) + "\n")
        f.write("\n")

        # Report on missing frames
        missing_frame_nums_in_acq = list(set(acquisition_frame_nums) & set(missing_frame_nums))
        missing_frame_nums_in_acq.sort()

        f.write(f"Total missing frames encountered in this acquisition: {len(missing_frame_nums_in_acq)}\n")
        f.write("List of missing frame numbers (if any):\n")
        if len(missing_frame_nums_in_acq) > 0:
            f.write("\n".join(i for i in missing_frame_nums_in_acq) + "\n")
        f.write("\n")

        # Report on cloudy frames
        cloudy_frame_nums.sort()
        f.write(f"Total cloudy frames encountered in this acquisition: {len(cloudy_frame_nums)}\n")
        f.write(f"List of cloudy frame numbers (if any):\n")
        if len(cloudy_frame_nums) > 0:
            f.write("\n".join(i for i in cloudy_frame_nums) + "\n")
        f.write("\n")

        # Report on corrupted lines (line count mismatch):
        f.write(f"Total corrupt lines (line count mismatches) in this acquisition: {len(corrupt_lines)}\n")
        f.write(f"List of corrupt lines (if any):\n")
        if len(corrupt_lines) > 0:
            for i, line_num in enumerate(corrupt_lines):
                f.write(f"{str(line_num).zfill(6)}\n")
        f.write("\n")

    result = {"corrupt_lines": corrupt_lines, "frame_corrupt_line_map": frame_corrupt_line_map}
    return result


def main():

    # Read in args
    parser = argparse.ArgumentParser(
        description="Description: This script executes the decompression and reassembly portion of the L1A PGE.\n"
                    "Operating Environment: Python 3.x. See setup.py file for specific dependencies.\n"
                    "Outputs:\n"
                    "    * Reassembled raw image file named <image_prefix>_raw.img\n"
                    "    * Reassembled raw header file named <image_prefix>_raw.hdr\n"
                    "    * PGE log file named reassemble_raw_pge.log (default)\n"
                    "    * Reassembly report file named reassemble_raw_pge_report.txt (default)\n",
        formatter_class=RawTextHelpFormatter)
    parser.add_argument("frames_dir", help="Frames directory path")
    parser.add_argument("--flexcodec_exe", help="Path to flexcodec exe")
    parser.add_argument("--constants_path", help="Path to constants.txt file")
    parser.add_argument("--init_data_path", help="Path to init_data.bin file")
    parser.add_argument("--interleave", help="Interleave setting for decompression - bil (default) or bip",
                        default="bil")
    parser.add_argument("--work_dir", help="Path to working directory", default=".")
    parser.add_argument("--level", help="Logging level", default="INFO")
    parser.add_argument("--log_path", help="Path to log file", default="reassemble_raw.log")
    parser.add_argument("--chunksize", help="Number of lines per output acquisition.", type=int, default=320000)
    parser.add_argument("--orbit", help="Orbit number (padded)", default="0000000")
    parser.add_argument("--scene", help="Scene in orbit number (padded)", default="000")
    parser.add_argument("--submode", help="Submode (science or dark)", default="science")
    parser.add_argument("--test_mode", action="store_true",
                        help="If enabled, don't throw errors regarding unprocessed or un-coadded data")

    args = parser.parse_args()

    # Upper case the log level
    args.level = args.level.upper()

    if not os.path.exists(args.work_dir):
        os.makedirs(args.work_dir)
    image_dir = os.path.join(args.work_dir, "image")
    if not os.path.exists(image_dir):
        os.makedirs(image_dir)

    # Set up console logging using root logger
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    logger = logging.getLogger("emit-sds-l1a")
    # Set up file handler logging
    handler = logging.FileHandler(args.log_path)
    handler.setLevel(args.level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Get frame paths
    frame_paths = glob.glob(os.path.join(args.frames_dir, "*[!txt]"))
    if len(frame_paths) == 0:
        raise RuntimeError(f"Could not find any frames in {args.frames_dir}. Unable to proceed with reassembly.")
    frame_paths.sort()

    # Get dcid and os time string
    dcid = os.path.basename(frame_paths[0]).split("_")[0]
    os_time_str = os.path.basename(frame_paths[0]).split("_")[1]

    # Start populating the report text to be written for each acquisition
    report_txt = "-----------------\n"
    report_txt += "REASSEMBLY REPORT\n"
    report_txt += "-----------------\n\n"
    report_txt += "DATA COLLECTION STATS\n"
    report_txt += "---------------------\n\n"
    report_txt += f"DCID: {dcid}\n"
    report_txt += f"Input frames directory: {args.frames_dir}\n"
    expected_frame_num_str = os.path.basename(frame_paths[0]).split("_")[3]
    report_txt += f"Total number of expected frames (from frame header): " \
        f"{int(expected_frame_num_str)}\n\n"

    report_txt += f"Orbit: {args.orbit}\n"
    report_txt += f"Scene: {args.scene}\n"
    report_txt += f"Submode: {args.submode}\n\n"

    # Set up various lists to track frame parameters (num bands, processed, coadd mode)
    frame_data_paths = []
    num_bands_list = []
    num_lines_list = []
    processed_flag_list = []
    compression_flag_list = []
    coadd_mode_list = []
    instrument_mode_list = []
    corrupt_frames_list = []
    failed_decompression_list = []
    uncompressed_list = []
    line_counts = [None] * int(expected_frame_num_str)
    start_times_gps = [None] * int(expected_frame_num_str)
    timing_info = [{"line_timestamp": -1, "os_time_timestamp": -1, "os_time": -1}
                   for x in range(int(expected_frame_num_str))]

    # Process frame headers and write out compressed data files
    for path in frame_paths:
        logger.info(f"Reading in frame {path}")
        with open(path, "rb") as f:
            frame_binary = f.read()
        frame = Frame(frame_binary)
        uncomp_frame_path = os.path.join(image_dir, os.path.basename(path) + ".xio.decomp")
        compression_flag_list.append(frame.compression_flag)

        # Check frame checksum
        logger.debug(f"Frame is valid: {frame.is_valid()}")

        # Check if frame is corrupt
        is_corrupt = False
        if os.path.basename(path).split(".")[0].split("_")[4] == "9":
            is_corrupt = True
            corrupt_frames_list.append(os.path.basename(path).split(".")[0].split("_")[2])

        # Decompress if compression flag is set and frame is not cloudy and not corrupt, otherwise, just copy file
        if frame.compression_flag == 1 and frame.cloudy_flag == 0 and not is_corrupt:
            # Decompress frame
            interleave_arg = "--" + args.interleave
            cmd = [args.flexcodec_exe, path, "-a", args.constants_path, "-i", args.init_data_path, "-v",
                   interleave_arg, "-o", uncomp_frame_path]
            cmd_str = " ".join(cmd)
            logger.info(f"Decompressing frame with command '{cmd_str}'")
            output = subprocess.run(" ".join(cmd), shell=True, capture_output=True)

            # Write output to log
            logger.info(output.stdout.decode("utf-8").replace("\\n", "\n").replace("\\t", "\t"))

            if output.returncode != 0 or \
                    "Segments decompressed successfully: 1 of 1" not in output.stdout.decode("utf-8"):
                logger.error(f"Failed to decompress frame with command '{cmd_str}'")
                failed_decompression_list.append(os.path.basename(path).split(".")[0].split("_")[2])
                # Remove attempted decompression path to avoid confusion
                if os.path.exists(uncomp_frame_path):
                    logger.error(f"Removing {uncomp_frame_path}")
                    os.remove(uncomp_frame_path)
                continue

        elif frame.compression_flag == 1 and is_corrupt:
            # Do nothing - this frame will fail decompression so skip it
            logger.info(f"Found compressed and corrupt frame at {path}. Not attempting to decompress or preserve.")
            continue

        else:
            # Just copy the uncompressed frame and rename it
            logger.info(f"Found uncompresssed or cloudy frame at {path}. Copying to {uncomp_frame_path}")
            shutil.copy2(path, uncomp_frame_path)
            uncompressed_list.append(os.path.basename(path).split(".")[0].split("_")[2])

        # Get some frame header details and write out uncompressed frame data section
        with open(uncomp_frame_path, "rb") as f:
            uncomp_frame_binary = f.read()
        uncomp_frame = Frame(uncomp_frame_binary)

        # Get start and stop times for each frame
        frame_num_index = int(os.path.basename(uncomp_frame_path).split(".")[0].split("_")[2])
        start_times_gps[frame_num_index] = uncomp_frame.start_time_gps

        # Get line count for each frame
        line_counts[frame_num_index] = uncomp_frame.line_count

        # Get timing info for each frame
        timing_info[frame_num_index] = {
            "line_timestamp": uncomp_frame.line_timestamp,
            "os_time_timestamp": uncomp_frame.os_time_timestamp,
            "os_time": uncomp_frame.os_time
        }

        num_bands_list.append(uncomp_frame.num_bands)
        processed_flag_list.append(uncomp_frame.processed_flag)
        instrument_mode_list.append(uncomp_frame.instrument_mode)
        # Num lines is only 64 in unprocessed frames where data size is 1280 * bands * 64 * 2
        size_of_64 = 1280 * uncomp_frame.num_bands * 64 * 2
        if uncomp_frame.processed_flag == 0 and uncomp_frame.data_size == size_of_64:
            num_lines_list.append(64)
        else:
            num_lines_list.append(32)
        coadd_mode_list.append(uncomp_frame.coadd_mode)

        uncomp_data_path = uncomp_frame_path + "_no_header"
        uncomp_frame.write_data(uncomp_data_path)

        # Add uncompressed data path to list of raw_frame_paths to be reassembled
        frame_data_paths.append(uncomp_data_path)

    # Update report with decompression stats
    failed_decompression_list.sort()
    uncompressed_list.sort()
    corrupt_frames_list.sort()

    # Abort if there is a mix of compressed and non-compressed
    compression_flag_list.sort()
    for i in range(len(compression_flag_list)):
        if compression_flag_list[i] != compression_flag_list[0]:
            raise RuntimeError(f"Not all frames have the same compression flag: {compression_flag_list}")
    compression_flag = compression_flag_list[0]

    # Check all frames have same number of bands
    # num_bands_list.sort()
    for i in range(len(num_bands_list)):
        if num_bands_list[i] != num_bands_list[0]:
            raise RuntimeError(f"Not all frames have the same number of bands. See list of num_bands: {num_bands_list}")

    # Check all frames have same number of lines
    # num_lines_list.sort()
    for i in range(len(num_lines_list)):
        if num_lines_list[i] != num_lines_list[0]:
            raise RuntimeError(
                f"Not all frames have the same number of lines. See list of num_lines: {num_lines_list}")

    # Check all frames have same instrument mode
    for i in range(len(instrument_mode_list)):
        if instrument_mode_list[i] != instrument_mode_list[0]:
            raise RuntimeError(
                f"Not all frames have the same instrument mode. See list of instrument modes: {instrument_mode_list}")
    instrument_mode = instrument_mode_list[0]

    # Abort if any of the frames are not processed (i.e. they are from the raw partition)
    processed_flag_list.sort()
    for processed_flag in processed_flag_list:
        if not args.test_mode and processed_flag == 0:
            raise RuntimeError(f"Some frames are not processed (processed flag is 0). See list of processed_flags: "
                               f"{processed_flag_list}")
    processed_flag = processed_flag_list[0]

    # Abort if coadd mode set to 0
    coadd_mode_list.sort()
    for coadd_mode in coadd_mode_list:
        if not args.test_mode and coadd_mode == 0:
            raise RuntimeError(f"Some frames are not coadded.  See list of coadd_mode flags: {coadd_mode_list}")
    coadd_mode = coadd_mode_list[0]

    # Get number of bands and lines
    num_bands = num_bands_list[0]
    num_lines = num_lines_list[0]
    if num_lines == 64:
        logger.warning(f"Frame has 64 lines! This is untested territory and should only apply to data that has not "
                       f"been processed!")

    # Calculate start/stop times for each frame
    # TODO: Raise error if only 1 data point
    start_stop_times = calculate_start_stop_times(start_times_gps)

    # Add empty decompressed frame files to fill in missing frame numbers
    raw_frame_nums = [int(os.path.basename(path).split("_")[2]) for path in frame_data_paths]
    # seq_frame_nums = list(range(raw_frame_nums[0], raw_frame_nums[0] + len(raw_frame_nums)))
    seq_frame_nums = list(range(0, int(os.path.basename(frame_data_paths[0]).split("_")[3])))
    missing_frame_nums = list(set(seq_frame_nums) - set(raw_frame_nums))
    # Convert to padded strings like other lists
    missing_frame_nums = [str(num).zfill(5) for num in missing_frame_nums]
    missing_frame_nums.sort()
    # Now remove failed decompression frame nums from missing frame nums list
    missing_frame_nums = list(set(missing_frame_nums) - set(failed_decompression_list))
    missing_frame_nums.sort()
    # Also remove the corrupt frames which are purposely missing
    missing_frame_nums = list(set(missing_frame_nums) - set(corrupt_frames_list))
    missing_frame_nums.sort()

    logger.debug(f"List of corrupt frame numbers (if any): {corrupt_frames_list}")
    logger.debug(f"List of failed decompression frame numbers (if any): {failed_decompression_list}")
    logger.debug(f"List of missing frame numbers (if any): {missing_frame_nums}")

    # Add missing paths into frame_data_paths list with acquisition status of "6" to indicate missing.
    for num in missing_frame_nums:
        frame_data_paths.append(
            os.path.join(image_dir, "_".join([dcid, start_stop_times[int(num)][0].strftime("%Y%m%dt%H%M%S"),
                                              num, expected_frame_num_str, "6"])))

    # Add failed decompressions into frame_data_paths list with acquisition status of "7" to indicate failed.
    for num in failed_decompression_list:
        frame_data_paths.append(
            os.path.join(image_dir, "_".join([dcid, start_stop_times[int(num)][0].strftime("%Y%m%dt%H%M%S"),
                                              num, expected_frame_num_str, "7"])))

    # If compressed, add corrupt paths into frame_data_paths list with acquisition status of "9" to indicate corrupt.
    if compression_flag == 1:
        for num in corrupt_frames_list:
            frame_data_paths.append(
                os.path.join(image_dir, "_".join([dcid, start_stop_times[int(num)][0].strftime("%Y%m%dt%H%M%S"),
                                                  num, expected_frame_num_str, "9"])))

    frame_data_paths.sort(key=lambda x: os.path.basename(x).split("_")[2])

    # Update report based on frames
    report_txt += f"Partition: {'processed' if processed_flag == 1 else 'raw'}\n\n"
    report_txt += f"Instrument mode: {instrument_mode}\n"
    report_txt += f"Instrument mode description: {INSTRUMENT_MODE_DESCRIPTIONS[instrument_mode]}\n\n"
    report_txt += f"Number of lines per frame: {num_lines}\n\n"

    # Loop through the frames and create acquisitions
    i = 0
    num_frames = len(frame_data_paths)
    if args.chunksize % num_lines != 0:
        raise RuntimeError(f"Chunksize of {args.chunksize} must be a multiple of {num_lines}")
    frame_chunksize = min(args.chunksize // num_lines, num_frames)
    report_txt += f"Chunksize provided by args: {args.chunksize} lines or {args.chunksize // num_lines} frames\n"
    report_txt += f"Chunksize used to to split up acquisitions: {frame_chunksize * num_lines} lines or " \
        f"{frame_chunksize} frames\n\n"
    logger.info(f"Using frame chunksize of {frame_chunksize} to split data collection into acquisitions.")
    total_corrupt_lines = 0
    combined_frame_corrupt_line_map = OrderedDict()
    # Only do the chunking if there is enough left over for another full chunk
    while i + (2 * frame_chunksize) <= num_frames:
        acq_data_paths = frame_data_paths[i: i + frame_chunksize]
        result = reassemble_acquisition(acq_data_paths=acq_data_paths,
                                        start_index=i,
                                        stop_index=i + frame_chunksize - 1,
                                        start_time=start_stop_times[i][0],
                                        stop_time=start_stop_times[i + frame_chunksize - 1][1],
                                        timing_info=timing_info,
                                        processed_flag=processed_flag,
                                        compression_flag=compression_flag,
                                        coadd_mode=coadd_mode,
                                        num_bands=num_bands,
                                        num_lines=num_lines,
                                        image_dir=image_dir,
                                        report_text=report_txt,
                                        corrupt_frames_list=corrupt_frames_list,
                                        failed_decompression_list=failed_decompression_list,
                                        uncompressed_list=uncompressed_list,
                                        missing_frame_nums=missing_frame_nums,
                                        logger=logger)
        i += frame_chunksize
        total_corrupt_lines += len(result["corrupt_lines"])
        for key, value in result["frame_corrupt_line_map"].items():
            combined_frame_corrupt_line_map[key] = value

    # There will be one left over at the end that is the frame_chunksize + remaining frames
    acq_data_paths = frame_data_paths[i:]
    result = reassemble_acquisition(acq_data_paths=acq_data_paths,
                                    start_index=i,
                                    stop_index=num_frames - 1,
                                    start_time=start_stop_times[i][0],
                                    stop_time=start_stop_times[num_frames - 1][1],
                                    timing_info=timing_info,
                                    processed_flag=processed_flag,
                                    compression_flag=compression_flag,
                                    coadd_mode=coadd_mode,
                                    num_bands=num_bands,
                                    num_lines=num_lines,
                                    image_dir=image_dir,
                                    report_text=report_txt,
                                    corrupt_frames_list=corrupt_frames_list,
                                    failed_decompression_list=failed_decompression_list,
                                    uncompressed_list=uncompressed_list,
                                    missing_frame_nums=missing_frame_nums,
                                    logger=logger)
    total_corrupt_lines += len(result["corrupt_lines"])
    for key, value in result["frame_corrupt_line_map"].items():
        combined_frame_corrupt_line_map[key] = value

    # Write out a report for the data collection as a whole
    dcid_report_path = os.path.join(args.work_dir, f"{dcid}_reassembly_report.txt")
    with open(dcid_report_path, "w") as f:
        f.write(report_txt)
        # Corrupt frames
        f.write(f"Total corrupt frames in this data collection: {len(corrupt_frames_list)}\n")
        f.write("List of corrupt frame numbers (if any):\n")
        if len(corrupt_frames_list) > 0:
            f.write("\n".join(i for i in corrupt_frames_list) + "\n")
        f.write("\n")
        # Decompression errors
        f.write(f"Total decompression errors in this data collection: {len(failed_decompression_list)}\n")
        f.write("List of frame numbers that failed decompression (if any):\n")
        if len(failed_decompression_list) > 0:
            f.write("\n".join(i for i in failed_decompression_list) + "\n")
        f.write("\n")
        # Missing frames
        f.write(f"Total missing frames in this data collection: {len(missing_frame_nums)}\n")
        f.write("List of missing frame numbers (if any):\n")
        if len(missing_frame_nums) > 0:
            f.write("\n".join(i for i in missing_frame_nums) + "\n")
        f.write("\n")
        # Cloudy frames
        cloudy_frame_nums = []
        for p in frame_data_paths:
            if int(os.path.basename(p).split("_")[4]) in (4, 5):
                cloudy_frame_nums.append(int(os.path.basename(p).split("_")[2]))
        cloudy_frame_nums = [str(num).zfill(5) for num in cloudy_frame_nums]
        cloudy_frame_nums.sort()
        f.write(f"Total cloudy frames in this data collection: {len(cloudy_frame_nums)}\n")
        f.write(f"List of cloudy frame numbers (if any):\n")
        if len(cloudy_frame_nums) > 0:
            f.write("\n".join(i for i in cloudy_frame_nums) + "\n")
        f.write("\n")
        # Corrupt Lines
        f.write(f"Total corrupt lines (line count mismatches) in this data collection: {total_corrupt_lines}\n")
        f.write(f"List of corrupt lines (if any):\n")
        for frame, line_nums in combined_frame_corrupt_line_map.items():
            if len(line_nums) > 0:
                line_nums.sort()
                f.write(f"{frame}: {line_nums}\n")

    logger.info("Done")


if __name__ == '__main__':
    main()
