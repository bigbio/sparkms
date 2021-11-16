#!/bin/bash

# Convert Spectrum data from Assays into Parquet

PYSPARK_PYTHON=./environment/bin/python \
/nfs/software/pride/spark/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
--master yarn \
--deploy-mode cluster \
--executor-memory 49g  --driver-memory 50g --executor-cores 5 --num-executors 50 \
--conf spark.driver.memoryOverhead=20480  --conf spark.executor.memoryOverhead=30480 --conf spark.driver.maxResultSize=0 \
--archives sparkms.tar.gz#environment \
sparkms/commands/analysis/json_to_parquet.py json-to-parquet -i '/user/cbandla/pride/assays/' -d 'spectra' -o '/user/cbandla/pride/parquets/spectra' > convert_spectra.log 2>&1 &

## Convert peptides

PYSPARK_PYTHON=./environment/bin/python \
/nfs/software/pride/spark/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
--master yarn \
--deploy-mode cluster \
--executor-memory 49g  --driver-memory 50g --executor-cores 5 --num-executors 50 \
--conf spark.driver.memoryOverhead=20480  --conf spark.executor.memoryOverhead=30480 --conf spark.driver.maxResultSize=0 \
--archives sparkms.tar.gz#environment \
sparkms/commands/analysis/json_to_parquet.py json-to-parquet -i '/user/cbandla/pride/assays/' -d 'peptide' -o '/user/cbandla/pride/parquets/peptides' > convert_peptides.log 2>&1 &

## Convert proteins

PYSPARK_PYTHON=./environment/bin/python \
/nfs/software/pride/spark/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
--master yarn \
--deploy-mode cluster \
--executor-memory 49g  --driver-memory 50g --executor-cores 5 --num-executors 50 \
--conf spark.driver.memoryOverhead=20480  --conf spark.executor.memoryOverhead=30480 --conf spark.driver.maxResultSize=0 \
--archives sparkms.tar.gz#environment \
sparkms/commands/analysis/json_to_parquet.py json-to-parquet -i '/user/cbandla/pride/assays/' -d 'protein' -o '/user/cbandla/pride/parquets/proteins' > convert_proteins.log 2>&1 &
