"""
This script takes in a folder of BAD (Broadcast Ancillary Data) STO files and concatenates the output into NetCDF.

Author: Winston Olson-Duvall, winston.olson-duvall@jpl.nasa.gov
"""

import argparse
import datetime
import glob
import logging
import numpy as np
import os

from argparse import RawTextHelpFormatter

import h5netcdf

from ait.core import dmc

J2000_OFFSET = 0


def lookup_header_indices(hdr):
    iss_pui_map = {
        "time_coarse": "LADP06MD2378W",
        "time_fine": "LADP06MD2380W",
        "time_error_sec": "LADP06MD2890W",
        "time_error_subsec": "LADP06MD2891W",
        "pos_x": "LADP06MD2395H",
        "pos_y": "LADP06MD2396H",
        "pos_z": "LADP06MD2397H",
        "vel_x": "LADP06MD2399R",
        "vel_y": "LADP06MD2400R",
        "vel_z": "LADP06MD2401R",
        "att_q0": "LADP06MD2382U",
        "att_q1": "LADP06MD2383U",
        "att_q2": "LADP06MD2384U",
        "att_q3": "LADP06MD2385U"
    }

    hdr_indices = {}
    for k, v in iss_pui_map.items():
        for i, field in enumerate(hdr):
            if v == field:
                hdr_indices[k] = i
    return hdr_indices


def get_utc_time_from_gps(gps_time):
    # Convert gps_time in seconds to a timestamp in utc
    d = dmc.GPS_Epoch + datetime.timedelta(seconds=gps_time)
    offset = dmc.LeapSeconds.get_GPS_offset_for_date(d)
    utc_time = d - datetime.timedelta(seconds=offset)
    return utc_time


def main():

    # Read in args
    parser = argparse.ArgumentParser(
        description="Description: This script creates a NetCDF formatted file of BAD data given STO-formatted BAD "
                    "files as input.\n"
                    "Operating Environment: Python 3.x. See setup.py file for specific dependencies.\n"
                    "Outputs:\n"
                    "    * A NetCDF formatted file containing BAD data.\n",
        formatter_class=RawTextHelpFormatter)
    parser.add_argument("bad_sto_dir", help="Path to a directory containing BAD files in STO format")
    parser.add_argument("--start_time", help="Start time for reformatted output file, YYYY-MM-DDTHH:MM:SS",
                        default="2000-01-01T00:00:00")
    parser.add_argument("--stop_time", help="Stop time for reformatted output file, YYYY-MM-DDTHH:MM:SS",
                        default="2050-01-01T00:00:00")
    parser.add_argument("--work_dir", help="Path to working directory", default=".")
    parser.add_argument("--level", help="Logging level", default="INFO")
    parser.add_argument("--log_path", help="Path to log file", default="reformat_bad.log")

    args = parser.parse_args()

    # Format args as needed
    args.level = args.level.upper()

    # Get start/stop times
    start_time = datetime.datetime.strptime(args.start_time, "%Y-%m-%dT%H:%M:%S")
    stop_time = datetime.datetime.strptime(args.stop_time, "%Y-%m-%dT%H:%M:%S")

    if not os.path.exists(args.work_dir):
        os.makedirs(args.work_dir)
    output_dir = os.path.join(args.work_dir, "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Construct output path
    output_path = os.path.join(output_dir, "emit_bad_att_eph.nc")

    # Set up console logging using root logger
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=args.level)
    logger = logging.getLogger("emit-sds-l1a")

    # Set up file handler logging
    handler = logging.FileHandler(args.log_path)
    handler.setLevel(args.level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(module)s]: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info(f"Processing STO files in {args.bad_sto_dir} using start time {args.start_time} and stop time "
                f"{args.stop_time}")
    sto_paths = glob.glob(os.path.join(args.bad_sto_dir, "*"))
    sto_paths.sort()
    if len(sto_paths) == 0:
        raise RuntimeError(f"Did not find any STO files in {args.bad_sto_dir} to process.")
    logger.info(f"Found {len(sto_paths)} file to process.")

    # Read in the STO files and store data in out_arr
    # TODO: Do I need to fill in missing data somehow?
    out_arr = []
    out_arr_lens = []
    for p in sto_paths:
        logger.info(f"Processing file {p}")

        header = None
        ind = None
        data_start = False
        with open(p, "r") as f:
            for line in f:
                # Handle the header (but don't write to NetCDF
                if "Header" in line and not header:
                    header = line.replace("status", " ").replace("\t \t", "\t").strip("\n").split("\t")
                    ind = lookup_header_indices(header)
                    # out_arr.append(header)
                    # out_file.writelines(",".join(header).rstrip(",") + "\n")
                    continue
                # Set or reset data_start to False
                if "End_Data" in line:
                    data_start = False
                # Read in the data
                if data_start:
                    if not line.startswith("#Data"):
                        continue
                    line_values = line.strip("\n").replace("\t \t", "\t").replace("\tS\t", "\t").replace("\tDS\t", "\t").\
                        replace("\t\t", "\t").split("\t")
                    # data = [str(v).strip(" ") for v in l]
                    data = []
                    for v in line_values:
                        if len(str(v).strip(" ")) == 0:
                            data.append(None)
                        else:
                            data.append(str(v).strip(" "))

                    # Check if data is in start/stop time range and if so, append to output array
                    if ind is None:
                        raise RuntimeError("Attempting to add data, but no header row has been found.")
                    timestamp = get_utc_time_from_gps(int(data[ind["time_coarse"]]))
                    if start_time <= timestamp <= stop_time:
                        out_arr.append(data)
                        out_arr_lens.append(len(data))
                        # out_file.writelines(",".join(data).rstrip(",") + "\n")
                # Set or reset data_start to True
                if "Start_Data" in line:
                    data_start = True

    # Throw error if no matching data was found
    if len(out_arr) == 0:
        raise RuntimeError(f"Failed to find any BAD data between orbit start time of {args.start_time} and stop time "
                           f"of {args.stop_time}")

    # ind = lookup_header_indices(header)
    out_arr.sort(key=lambda x: x[ind["time_coarse"]])

    min_time = int(out_arr[0][ind["time_coarse"]])
    max_time = int(out_arr[-1][ind["time_coarse"]])

    # Create NetCDF file and write out selected fields
    fout = h5netcdf.File(output_path, "w")
    # TODO: J2000 conversion
    tm = np.asarray([float(row[ind["time_coarse"]]) + float(row[ind["time_fine"]] + J2000_OFFSET)
                     for row in out_arr], dtype=np.float)
    pos = np.zeros((tm.shape[0], 3))
    vel = np.zeros((tm.shape[0], 3))
    quat = np.zeros((tm.shape[0], 4))

    for i, row in enumerate(out_arr):
        pos[i, :] = (row[ind["pos_x"]], row[ind["pos_y"]], row[ind["pos_z"]])
        vel[i, :] = (row[ind["vel_x"]], row[ind["vel_y"]], row[ind["vel_z"]])
        quat[i, :] = (row[ind["att_q0"]], row[ind["att_q1"]], row[ind["att_q2"]], row[ind["att_q3"]])

    g = fout.create_group("Ephemeris")
    t = g.create_variable("time_j2000", ('t',), data=tm)
    t.attrs["units"] = "s"
    t = g.create_variable("eci_position", ('t', 'position'), data=pos)
    t.attrs["description"] = "ECI position"
    t.attrs["units"] = "m"
    t = g.create_variable("eci_velocity", ('t', 'velocity'), data=vel)
    t.attrs["description"] = "ECI velocity"
    t.attrs["units"] = "m/s"

    g = fout.create_group("Attitude")
    t = g.create_variable("time_j2000", ('t',), data=tm)
    t.attrs["units"] = "s"
    t = g.create_variable("quaternion", ('t', 'quaternion'), data=quat)
    t.attrs["description"] = "Attitude quaternion, goes from spacecraft to ECI. The coefficient convention used has " \
                             "the real part in the first column."
    t.attrs["units"] = "dimensionless"

    fout.close()
    logger.info("Done.")


if __name__ == '__main__':
    main()
