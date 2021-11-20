from typing import Final
import re

import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf, desc
from pyspark.sql.types import DoubleType
from pyopenms import *

from sparkms.commons import Fields


def hyper_score(usi, peptide, charge, masses, intensities):
  score = 0.0
  if masses == None or intensities == None or len(masses) < 15 or len(intensities) != len(masses):
    return 0.0

  spectrum = MSSpectrum()
  # print(masses)
  # print(intensities)
  # print(modifications)

  try:
    # convert masses from string to float
    masses = np.array(masses)
    masses = masses.astype(np.float)

    # convert masses from string to float
    intensities = np.array(intensities)
    intensities = intensities.astype(np.float)

    spectrum.set_peaks([masses, intensities])

    # Theroretical spectrum
    tsg = TheoreticalSpectrumGenerator()
    thspec = MSSpectrum()
    p = Param()
    p.setValue("add_metainfo", "true")
    tsg.setParameters(p)
    peptide = AASequence.fromString(peptide)

    tsg.getSpectrum(thspec, peptide, 1, int(charge))

    score_engine = HyperScore()
    score = score_engine.compute(20, True, spectrum, thspec)
    # print(usi + " score: " + str(score))
  except:
    print("Spectrum {} -- masses {} -- intensities {}".format(usi, masses, intensities))

  return score

@click.command('spectrum-ion-annotation', short_help='')
@click.option('-spectra', help="Input spectra parquet files. ie., /path/to/", required=True)
@click.option('-o', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def spectrum_ion_annotation(spectra, out_path):
  # Create the Spark Context
  sql_context = SparkSession.builder.getOrCreate()

  # Read the psms and peptide into a dataset
  df_spectra = sql_context.read.parquet(spectra)

  udf_hyper_score = udf(hyper_score, DoubleType())
  df_spectra = df_spectra.withColumn('HyperScore', udf_hyper_score('usi', 'peptideSequence', 'precursorCharge', 'masses','intensities'))

  df_psm_final = df_spectra.select("usi", "peptideSequence", "numPeaks", 'HyperScore', explode('properties').alias(Fields.ADDITIONAL_ATTRIBUTES))
  df_psm_final = df_psm_final.filter("additionalAttributes.accession == 'MS:1002355'").select(Fields.USI, "peptideSequence", "numPeaks", 'HyperScore', col('additionalAttributes.value').cast('float').alias('fdrscore')).sort(desc("HyperScore"))

  df_psm_final.write.parquet(out_path, mode='append', compression='snappy')

  # This is important if you print before writing the analysis code is executed twice.

  df_print = sql_context.read.parquet(out_path)
  df_print.show(truncate=False, n=30000)

if __name__ == '__main__':
    spectrum_ion_annotation()
