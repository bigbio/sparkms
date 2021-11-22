from typing import Final
import re

import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import size, col, desc

@click.command('peptide-unknown', short_help='')
@click.option('--peptide-folder', help="Input peptide summary folder in parquet files. ie., /path/to/", required=True)
@click.option('--fdr-threshold', help = 'Maximum FDR Score allowed', default = 0.01)
@click.option('--out-peptide-file', help="Output path to store parquets. ie., /out/path", required=True)
def peptide_summary_unknown(peptide_folder, fdr_threshold, out_peptide_file):
  """
  This function read the peptide parquet output and select all the peptides that are not mapped to uniprot accessions and
  create a table with the peptide sequence and the original protein accession reported.

  :param peptide_folder: Folder containing all the peptides annotated after groping with the peptide_summary.py tool
  :param fdr_threshold: The maximum fdr value that will be allow for the best search engine score.
  :param out_peptide_file: A Tsv file containing all the unmpapped peptides to uniprot accessions.
  :return:
  """


  # Create the Spark Context
  sql_context = SparkSession.builder.getOrCreate()
  df_pep_original = sql_context.read.parquet(peptide_folder)

  df_pep_original_non_uniprot =  df_pep_original.filter(df_pep_original.is_uniprot_accession == False)\
     .filter(df_pep_original.best_search_engine_score <= fdr_threshold).select(col("peptideSequence"), col("proteinAccession")).distinct()
  df_pep_original_non_uniprot.show(n=300)

  df_pep_original_non_uniprot.toPandas().to_csv(out_peptide_file, sep='\t', header=True, index=False)


if __name__ == '__main__':
    peptide_summary_unknown()
