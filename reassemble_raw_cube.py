"""
This script reassembles the raw image cube from acquisition frames with uncompressed frame headers and compressed
frame data.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import glob
import logging
import os
import shutil
import subprocess

from argparse import RawTextHelpFormatter

import numpy as np
import spectral.io.envi as envi

from emit_sds_l1a.frame import Frame

# TODO: Different flags for missing frame vs. missing portion of frame?
MISSING_DATA_FLAG = -9998
CLOUDY_DATA_FLAG = -9997


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
    parser.add_argument("--out_dir", help="Output directory", default=".")
    parser.add_argument("--level", help="Logging level", default="INFO")
    parser.add_argument("--log_path", help="Path to log file", default="reassemble_raw.log")
    parser.add_argument("--test_mode", action="store_true",
                        help="If enabled, don't throw errors regarding unprocessed or un-coadded data")

    args = parser.parse_args()

    # Upper case the log level
    args.level = args.level.upper()

    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

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
    frame_paths = glob.glob(os.path.join(args.frames_dir, "*"))
    frame_paths.sort()

    # Create a reassembly report
    report_path = args.log_path.replace(".log", "_report.txt")
    report_file = open(report_path, "w")
    report_file.write("REASSEMBLY REPORT\n")
    report_file.write("-----------------\n\n")
    report_file.write(f"Input frames directory: {args.frames_dir}\n")
    expected_frame_num_str = os.path.basename(frame_paths[0]).split("_")[2]
    report_file.write(f"Total number of expected frames (from frame header): {int(expected_frame_num_str)}\n\n")

    # Set up various lists to track frame parameters (num bands, processed, coadd mode)
    frame_data_paths = []
    num_bands_list = []
    processed_flag_list = []
    coadd_mode_list = []
    failed_decompression_list = []
    uncompressed_list = []

    # Process frame headers and write out compressed data files
    for path in frame_paths:
        logger.info(f"Reading in frame {path}")
        with open(path, "rb") as f:
            frame_binary = f.read()
        frame = Frame(frame_binary)
        uncomp_frame_path = os.path.join(args.out_dir, os.path.basename(path) + ".xio.decomp")

        # Check frame checksum
        logger.debug(f"Frame is valid: {frame.is_valid()}")

        # Decompress if compression flag is set, otherwise, just copy file
        if frame.compression_flag == 1:
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
                failed_decompression_list.append(os.path.basename(path).split(".")[0].split("_")[1])
                # Remove attempted decompression path to avoid confusion
                if os.path.exists(uncomp_frame_path):
                    logger.error(f"Removing {uncomp_frame_path}")
                    os.remove(uncomp_frame_path)
                continue
                # raise RuntimeError(output.stderr.decode("utf-8"))

        else:
            # Just copy the uncompressed frame and rename it
            logger.info(f"Found uncompresssed frame at {path}. Copying to {uncomp_frame_path}")
            shutil.copy2(path, uncomp_frame_path)
            uncompressed_list.append(os.path.basename(path).split(".")[0].split("_")[1])

        # Get some frame header details and write out uncompressed frame data section
        with open(uncomp_frame_path, "rb") as f:
            uncomp_frame_binary = f.read()
        uncomp_frame = Frame(uncomp_frame_binary)

        num_bands_list.append(uncomp_frame.num_bands)
        processed_flag_list.append(uncomp_frame.processed_flag)
        coadd_mode_list.append(uncomp_frame.coadd_mode)

        uncomp_data_path = uncomp_frame_path + "_no_header"
        uncomp_frame.write_data(uncomp_data_path)

        # Add uncompressed data path to list of raw_frame_paths to be reassembled
        frame_data_paths.append(uncomp_data_path)

    # Update report with decompression stats
    failed_decompression_list.sort()
    uncompressed_list.sort()
    report_file.write(f"Total decompression errors encountered: {len(failed_decompression_list)}\n")
    report_file.write("List of frame numbers that failed decompression (if any):\n")
    if len(failed_decompression_list) > 0:
        report_file.write("\n".join(i for i in failed_decompression_list) + "\n")
    report_file.write(f"\nTotal number of frames not requiring decompression (compression flag set to 0): "
                      f"{len(uncompressed_list)}\n")
    report_file.write("List of frame numbers not requiring decompression (if any):\n")
    if len(uncompressed_list) > 0:
        report_file.write("\n".join(i for i in uncompressed_list) + "\n")

    # Check all frames have same number of bands
    num_bands_list.sort()
    for i in range(len(num_bands_list)):
        if num_bands_list[i] != num_bands_list[0]:
            raise RuntimeError(f"Not all frames have the same number of bands. See list of num_bands: {num_bands_list}")

    # Abort if any of the frames are not processed (i.e. they are from the raw partition)
    processed_flag_list.sort()
    for processed_flag in processed_flag_list:
        if not args.test_mode and processed_flag == 0:
            raise RuntimeError(f"Some frames are not processed (processed flag is 0). See list of processed_flags: "
                               f"{processed_flag_list}")

    # Abort if coadd mode set to 0
    coadd_mode_list.sort()
    for coadd_mode in coadd_mode_list:
        if not args.test_mode and coadd_mode == 0:
            raise RuntimeError(f"Some frames are not coadded.  See list of coadd_mode flags: {coadd_mode_list}")

    # Add empty decompressed frame files to fill in missing frame numbers
    raw_frame_nums = [int(os.path.basename(path).split("_")[1]) for path in frame_data_paths]
    # seq_frame_nums = list(range(raw_frame_nums[0], raw_frame_nums[0] + len(raw_frame_nums)))
    seq_frame_nums = list(range(0, int(os.path.basename(frame_data_paths[0]).split("_")[2])))
    missing_frame_nums = list(set(seq_frame_nums) - set(raw_frame_nums))
    missing_frame_nums.sort()
    logger.debug(f"List of missing frame numbers (if any): {missing_frame_nums}")

    report_file.write(f"\nTotal missing frames encountered: {len(missing_frame_nums)}\n")
    report_file.write("List of missing frame numbers (if any):\n")
    if len(missing_frame_nums) > 0:
        report_file.write("\n".join(str(i).zfill(5) for i in missing_frame_nums) + "\n")

    acquisition_id = os.path.basename(frame_data_paths[0]).split("_")[0]
    # expected_frame_num_str = os.path.basename(frame_data_paths[0].split("_")[2])
    for frame_num_str in missing_frame_nums:
        frame_data_paths.append(os.path.join(args.out_dir, "_".join([acquisition_id, str(frame_num_str).zfill(5),
                                                                    expected_frame_num_str, "6"])))
    frame_data_paths.sort()

    # Reassemble frames into ENVI image cube filling in missing and cloudy data with data flags
    hdr_path = os.path.join(args.out_dir, acquisition_id + "_raw.hdr")
    num_lines = 32 * len(frame_data_paths)
    hdr = {
        "description": "EMIT L1A raw instrument data (units: DN)",
        "samples": 1280,
        "lines": num_lines,
        "bands": num_bands_list[0],
        "header offset": 0,
        "file type": "ENVI",
        "data type": 2,
        "interleave": "bil",
        "byte order": 0
    }

    envi.write_envi_header(hdr_path, hdr)
    out_file = envi.create_image(hdr_path, hdr, ext="img", force=True)
    output = out_file.open_memmap(interleave="source", writable=True)
    output[:, :, :] = -9999

    logger.debug(f"Assembling frames into raw file with header {hdr_path}")
    cloudy_frame_nums = []
    line = 0
    for path in frame_data_paths:
        frame_num_str = os.path.basename(path).split(".")[0].split("_")[1]
        status = int(os.path.basename(path).split(".")[0].split("_")[3])
        logger.debug(f"Adding frame {path}")
        # Non-cloudy frames
        if status in (0, 1):
            frame = np.memmap(path, shape=(32, int(hdr["bands"]), int(hdr["samples"])), dtype=np.uint16, mode="r")
            output[line:line + 32, :, :] = frame[:, :, :].copy()
        # Cloudy frames
        if status in (4, 5):
            cloudy_frame_nums.append(frame_num_str)
            frame = np.full(shape=(32, int(hdr["bands"]), int(hdr["samples"])), fill_value=CLOUDY_DATA_FLAG,
                            dtype=np.uint16)
            output[line:line + 32, :, :] = frame[:, :, :].copy()
        # Missing frames
        if status == 6:
            frame = np.full(shape=(32, int(hdr["bands"]), int(hdr["samples"])), fill_value=MISSING_DATA_FLAG,
                            dtype=np.uint16)
            output[line:line + 32, :, :] = frame[:, :, :].copy()
        line += 32
    del output

    report_file.write(f"\nTotal cloudy frames encountered: {len(cloudy_frame_nums)}\n")
    report_file.write(f"List of cloudy frame numbers (if any):\n")
    if len(cloudy_frame_nums) > 0:
        report_file.write("\n".join(i for i in cloudy_frame_nums))
    report_file.close()
    logger.info("Done")


if __name__ == '__main__':
    main()
