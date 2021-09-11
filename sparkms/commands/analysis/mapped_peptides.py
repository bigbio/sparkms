from typing import Final
import re

import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import size, col, desc
from pyspark.sql import functions as F
import pyspark.sql.functions as f
import pandas as pd

from sparkms.commons.Fields import PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS,\
  PROJECTS_COUNT, TAXID

def procees_peptide_accessions(row):
  result = list()
  result.append(row[0])
  value = None
  if(len(row[1][1].split(" ")) == 1):
    value = row[1][1]
  print(row)
  return result

@click.command('peptide-uknown', short_help='')
@click.option('--peptide-mapped-file', help="Input peptide summary mapped file. ie., /path/to/", required=True)
@click.option('--out-peptide-file', help="Output path to store parquets. ie., /out/path", required=True)
def peptide_summary_uknown(peptide_mapped_file, out_peptide_file):
  """
  The peptide summary to uniprot input files takes the parquet output from the peptide summary analysis pipeline
  into a uniprot output file format that can be use to push data to uniprot.

  :param peptide_folder: Folder containing all the peptides
  :param out_uniprot_folder: Output folder containing all the uniprot files
  :return:
  """


  df_pep_original = pd.read_csv(peptide_mapped_file, sep='\t')
  df_pep_original = df_pep_original.groupby("peptide").agg({'protein':lambda x: list(x)})
  df_pep_original['protein_count'] = df_pep_original['protein'].str.len()
  df_pep_original = df_pep_original[df_pep_original['protein_count'] == 1]
  df_pep_original['protein_count'] = df_pep_original.protein.apply(lambda x: len(x[0].split()))
  df_pep_original = df_pep_original[df_pep_original['protein_count'] == 1]
  print(df_pep_original.head(10))
  #   .agg(f.collect_list("protein").alias("proteins"))
  # df_pep_original = df_pep_original.withColumn("protein_count", size(df_pep_original.proteins))\
  #   .filter(col("protein_count") == 1)

  df_pep_original.to_csv(out_peptide_file, sep='\t', header=True)


if __name__ == '__main__':
    peptide_summary_uknown()
