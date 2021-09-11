from typing import Final
import re

import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import size, col, desc
from pyspark.sql import functions as F

from sparkms.commons.Fields import PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS,\
  PROJECTS_COUNT, TAXID

@click.command('peptide-uknown', short_help='')
@click.option('--peptide-folder', help="Input peptide summary folder in parquet files. ie., /path/to/", required=True)
@click.option('--fdr-threshold', help = 'Maximum FDR Score allowed', default = 0.01)
@click.option('--out-peptide-file', help="Output path to store parquets. ie., /out/path", required=True)
def peptide_summary_uknown(peptide_folder, fdr_threshold, out_peptide_file):
  """
  The peptide summary to uniprot input files takes the parquet output from the peptide summary analysis pipeline
  into a uniprot output file format that can be use to push data to uniprot.

  :param peptide_folder: Folder containing all the peptides
  :param out_uniprot_folder: Output folder containing all the uniprot files
  :return:
  """

  # Create the Spark Context
  sql_context = SparkSession.builder.getOrCreate()
  df_pep_original = sql_context.read.parquet(peptide_folder)

  df_pep_original_non_uniprot =  df_pep_original.filter(df_pep_original.is_uniprot_accession == False)\
     .filter(df_pep_original.best_search_engine_score <= fdr_threshold).select(col("peptideSequence")).distinct()
  df_pep_original_non_uniprot.show(n=300)

  df_pep_original_non_uniprot.toPandas().to_csv(out_peptide_file, sep='\t', header=True, index=False)


if __name__ == '__main__':
    peptide_summary_uknown()
