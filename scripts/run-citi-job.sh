#!/bin/bash

set -e

if [[ $1 -eq "ingest" ]]; then
    JOBFILE="jobs/citibike_ingest.py"
    OUTPUT_KEY="citibike_ingest"
elif [[ $1 -eq "distance" ]]; then
    JOBFILE="jobs/citibike_distance_calculation.py"
    OUTPUT_KEY="citibike_distance"
else
    echo "Invalid arg. Please fix and try again."
    exit 1
fi

poetry build

START_TS=$(date +%s)

set -x

if [ $(whoami) == dimas ]; then
    rm -rf ".output"

    poetry run spark-submit \
        --master local \
        --py-files dist/data_transformations-*.whl \
        $JOBFILE \
        "./resources/citibike/citibike.csv" \
        ".output"
else
    export SPARK_HOME=/usr/lib/spark
    poetry run spark-submit \
        --py-files dist/data_transformations-*.whl \
        $JOBFILE \
        "s3://dimas-thoughtworks-interview/resources/citibike/citibike.csv" \
        "s3://dimas-thoughtworks-interview/citibike_output/$OUTPUT_KEY/$START_TS"
fi