# sparkms

PySpark package for analysis of proteomics big data.

[![Python application](https://github.com/bigbio/sparkms/actions/workflows/python-app.yml/badge.svg?branch=main)](https://github.com/bigbio/sparkms/actions/workflows/python-app.yml)

[![Unit testing workflow](https://github.com/bigbio/sparkms/actions/workflows/unit_tests.yml/badge.svg?branch=main)](https://github.com/bigbio/sparkms/actions/workflows/unit_tests.yml)


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

## Issues

https://github.com/bigbio/sparkms/issues

## Contributing

Contributing to this repo should be done in the [dev branch](https://github.com/bigbio/sparkms/tree/dev).

