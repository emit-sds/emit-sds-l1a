# emit-sds-l1a


## Description

Welcome to the emit-sds-l1a repository.  This repository contains scripts for executing the various EMIT L1A PGEs.  These PGEs include the following functions:
* Depacketizing science frames from CCSDS stream files
* Depacketizing engineering data from CCSDS
* Decompressing and reassembling raw image files in ENVI format
* Reformatting BAD STO files into NetCDF files

To understand how this repository is linked to the rest of the emit-sds repositories, please see the [repository guide](https://github.com/emit-sds/emit-main/wiki/Repository-Guide).

## Dependency Requirements

This repository is based on Python 3.x.  See `emit-sds-l1a/setup.py` for specific dependencies.

## Installation Instructions

Clone the repository:
```
git clone https://github.jpl.nasa.gov/emit-sds/emit-sds-l1a.git
```
Run pip install:
```
cd emit-sds-l1a
pip install -e .
```
Clone the emit-ios repository
```
git clone https://github.jpl.nasa.gov/emit/emit-ios.git
```
Install the emit-ios repository
```
cd emit-ios
pip install -e .
```

## Example Execution Commands

### Depacketizing Science Frames

```
python depacketize_science_frames.py <ccsds_path>
```

### Depacketizing Engineering Data

```
./run_l1a_eng.sh <ccsds_path> <work_dir> <edp_exe>
```
Where the "edp_exe" is the path to the [engineering data processor executable](https://github.jpl.nasa.gov/emit/emit-ios/blob/master/emit/bin/emit_l1_edp.py).

### Decompressing and Reassembling Raw Images

```
python reasssemble_raw_cube.py <frames_dir> --flexcodec_exe <flexcodec_exe> --constants_path <constants_path> --init_data_path <init_data_path>
```
Where the "flexcodec_exe" is the path to the [decompression executable](https://github.jpl.nasa.gov/flex-data-compression/EMIT_FLEX_codec) 
and the [constants_path](https://github.jpl.nasa.gov/emit-sds/emit-sds-l1a/blob/main/decompression/constants.txt) and 
[init_data_path](https://github.jpl.nasa.gov/emit-sds/emit-sds-l1a/blob/main/decompression/FPGA_Data_Initialization_File_CREATE_COMPRESSION_INIT_DATA_328_e0.bin) 
are decompression files. 

### Reformatting BAD STO Files

```
python reformat_bad.py <bad_sto_dir>
```