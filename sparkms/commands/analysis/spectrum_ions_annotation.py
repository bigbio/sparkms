from typing import Final
import re

import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import length
from pyspark.sql.functions import map_from_entries
from pyspark.sql.functions import size
from pyspark.sql.functions import struct
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BooleanType, DoubleType
from pyopenms import *


def hyper_score(peptide, charge, modifications, mz, masses, intensities):

  if len(masses) < 10 or len(intensities) != len(masses):
    return 0

  spectrum = MSSpectrum()
  # print(masses)
  # print(intensities)
  spectrum.set_peaks([masses, intensities])
  print(spectrum[0].getMZ(), spectrum[0].getIntensity())

  return None

@click.command('spectrum-ion-annotation', short_help='')
@click.option('-spectra', help="Input spectra parquet files. ie., /path/to/", required=True)
@click.option('-o', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def spectrum_ion_annotation(spectra, out_path):
  # Create the Spark Context
  sql_context = SparkSession.builder.getOrCreate()

  # Read the psms and peptide into a dataset
  df_spectra = sql_context.read.parquet(spectra)

  udf_hyper_score = udf(hyper_score, DoubleType())
  df_spectra.withColumn('HyperScore', udf_hyper_score('peptideSequence', 'precursorCharge', 'modifications', 'precursorMz','masses','intensities')).show()


  df_spectra.show(n=300)








if __name__ == '__main__':
    spectrum_ion_annotation()
