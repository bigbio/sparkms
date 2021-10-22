# sparkms

PySpark package for analysis of proteomics big data.

[![Python application](https://github.com/bigbio/sparkms/actions/workflows/python-app.yml/badge.svg?branch=main)](https://github.com/bigbio/sparkms/actions/workflows/python-app.yml) [![Unit testing workflow](https://github.com/bigbio/sparkms/actions/workflows/unit_tests.yml/badge.svg?branch=main)](https://github.com/bigbio/sparkms/actions/workflows/unit_tests.yml)

## Description

The sparkMS package allows to perform data analysis of MS-based proteomics data large scale. The package is use by PRIDE database (www.ebi.ac.uk/pride) to perform multiple tasks across public proteomics datasets.

## Prerequisites and Software versions

- Spark **(3.0.0)**
- pyspark==3.0.0
- pyarrow==2.0.0
- pyopenms

**Note**: The software versions are crucial for the package and scripts to work. An small change in the versions of Spark or pyspark can make the tool fail.

## Some functionalities:

- Aggregate peptide spectrum matches across multiple experiments to obtain peptide evidences.
- Aggregate all the peptide evidences across datasets for each protein evidence.

## Building the conda environment

Building the conda environment

```asciidoc
conda env create -f environment.yml
```

Installing conda-pack

```bash
conda install conda-pack
conda install -c conda-forge conda-pack
```

Packing the environment

```asciidoc
conda pack -n my_env -o out_name.tar.gz
```

## Running sparkMS in EBI cluster

```asciidoc
PYSPARK_PYTHON=./environment/bin/python \
/nfs/software/pride/spark/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
--master yarn \
--deploy-mode cluster \
--executor-memory 49g  --driver-memory 50g --executor-cores 5 --num-executors 50 \
--conf spark.driver.memoryOverhead=20480  --conf spark.executor.memoryOverhead=30480 --conf spark.driver.maxResultSize=0 \
--archives sparkms.tar.gz#environment \
sparkms/commands/analysis/peptide_summary.py -pep '/user/cbandla/pride/assays_parquets_partitioned/peptide_evidences' -psm '/user/cbandla/pride/assays_parquets_partitioned/psm_summary_evidences' --uniprot-map '/user/cbandla/pride/uniprot_mappings/parquet/' --single-protein-map '/user/cbandla/pride/single_protein_map/parquet/' -o '/user/cbandla/pride/analysis/peptide_summary/' > peptide_summary.log 2>&1 &
```

## Issues

https://github.com/bigbio/sparkms/issues

## Contributing

Contributing to this repo should be done in the [dev branch](https://github.com/bigbio/sparkms/tree/dev).

