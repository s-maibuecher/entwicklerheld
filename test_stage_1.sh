#!/bin/bash

report_file="results.xml"
error_file="error.txt"

python -m waterpumps.tests > ${error_file} 2>&1
if [ -e ${report_file} ]
then
    cat ${report_file}
else
    cat ${error_file}
fi
