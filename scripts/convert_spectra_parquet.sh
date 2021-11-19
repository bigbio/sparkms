#!/bin/bash

source /nfs/software/pride/scripts/export_env_vars

# Convert Spectrum data from Assays into Parquet

PYSPARK_PYTHON=./environment/bin/python \
/nfs/software/pride/spark/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
--master yarn \
--deploy-mode cluster \
--executor-memory 49g  --driver-memory 50g --executor-cores 5 --num-executors 50 \
--conf spark.driver.memoryOverhead=20480  --conf spark.executor.memoryOverhead=30480 --conf spark.driver.maxResultSize=0 \
--archives /homes/pst_prd/tools/sparkms.tar.gz#environment \
/homes/pst_prd/tools/sparkms/sparkms/commands/converters/json_to_parquet.py -d 'spetra' -i '/user/pst_prd/pride/assays/' -o '/user/pst_prd/pride/parquets/spectra' > convert_spectra.log 2>&1 &
