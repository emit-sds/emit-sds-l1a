#!/bin/bash
 
CCSDS_PATH=$1
L1_PROC_DIR=$2
L1_EDP_EXE=$3
L1_INPUT="${L1_PROC_DIR}/input"
L1_OUTPUT="${L1_PROC_DIR}/output"
L1_LOGS="${L1_PROC_DIR}/logs"
 
# rm -rf ${L1_PROC_DIR}
mkdir -p ${L1_INPUT}
mkdir -p ${L1_OUTPUT}
mkdir -p ${L1_LOGS}

cp -v ${CCSDS_PATH} ${L1_INPUT}
in_file_name=`basename ${CCSDS_PATH} | sed 's/_l0_ccsds.bin//'`
`python ${L1_EDP_EXE} --input-dir=${L1_INPUT} --output-dir=${L1_OUTPUT} --log-dir=${L1_LOGS}`
mv ${L1_OUTPUT}/0x15_edd.csv ${L1_OUTPUT}/${in_file_name}_l1a_eng.csv
mv ${L1_OUTPUT}/l1_pge_report.txt ${L1_OUTPUT}/${in_file_name}_l1a_eng_report.txt
