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
/homes/pst_prd/tools/sparkms/sparkms/commands/converters/tsv_to_parquet.py -psm '/user/pst_prd/pride/parquets/spectra_ions/' -pep  '/user/pst_prd/pride/parquests/peptides/' \
 --uniprot-map '/user/pst_prd/pride/parquets/uniprot_swissprot/' --single-protein-map '/user/pst_prd/pride/parquets/peptide_single/' -o '/user/pst_prd/pride/parquets/peptide_maps/' > peptide_analysis.log 2>&1 &
