from typing import Final
import re

import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import size, col, desc
from pyspark.sql import functions as F

from sparkms.commons.Fields import PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS,\
  PROJECTS_COUNT, TAXID

@click.command('peptide-summary-blast-uniprot', short_help='')
@click.option('--peptide-folder', help="Input peptide summary folder in parquet files. ie., /path/to/", required=True)
@click.option('--uniprot-tsv', help='Uniprot tsv file folder', required=True)
@click.option('--fdr-threshold', help = 'Maximum FDR Score allowed', default = 0.01)
@click.option('--out-uniprot-folder', help="Output path to store parquets. ie., /out/path", required=True)
def peptide_summary_blast_uniprot(peptide_folder, uniprot_tsv, fdr_threshold, out_uniprot_folder):
  """
  The peptide summary to uniprot input files takes the parquet output from the peptide summary analysis pipeline
  into a uniprot output file format that can be use to push data to uniprot.

  :param peptide_folder: Folder containing all the peptides
  :param out_uniprot_folder: Output folder containing all the uniprot files
  :return:
  """

  # Create the Spark Context
  sql_context = SparkSession.builder.getOrCreate()

  print("======= processing:" + uniprot_tsv)
  uniprot_fasta = sql_context.read.csv(uniprot_tsv + "*.tsv", sep=r'\t', header=True)
  uniprot_fasta.show(n =30)

  # Read the psms and peptide into a dataset
  df_pep_original = sql_context.read.parquet(peptide_folder)

  df_pep_original_non_uniprot =  df_pep_original.filter(df_pep_original.is_uniprot_accession == False)\
     .filter(df_pep_original.best_search_engine_score <= fdr_threshold)

  mapped_peptides = df_pep_original_non_uniprot.join(uniprot_fasta, on=uniprot_fasta["Sequence"].contains(df_pep_original_non_uniprot["peptideSequence"])) \
    .groupBy("Accession").agg(F.first(uniprot_fasta["Sequence"]), F.collect_list("Accession")
  )

  mapped_peptides.show(n = 30)

  # df_pep_original_non_uniprot =  df_pep_original.filter(df_pep_original.is_uniprot_accession == False)\
  #   .filter(df_pep_original.best_search_engine_score <= fdr_threshold)\
  #   .select(col("proteinAccession"))

  df_pep_original_non_uniprot = df_pep_original_non_uniprot.groupBy('proteinAccession').count()
  df_pep_original_non_uniprot = df_pep_original_non_uniprot.sort(desc("count"))
  df_pep_original_non_uniprot.show(n=300)

  df_pep_original_non_uniprot.toPandas().to_csv(out_uniprot_folder + 'non_uniprot_good.tsv', sep='\t', header=True, index=False)

  df_pep_original = df_pep_original.filter(df_pep_original.is_uniprot_accession == True)\
    .filter(df_pep_original.best_search_engine_score <= fdr_threshold).filter(df_pep_original.TaxId.isNotNull())\
    .withColumn(PROJECTS_COUNT, size(df_pep_original.projectAccessions))

  uniprot_columns = [PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS, PROJECTS_COUNT, TAXID]
  df_pep_original = df_pep_original.select(*uniprot_columns)
  df_pep_original.show(truncate=False, n=300)

  # df_pep_original.repartition(TAXID).write.option("header", "true").mode('overwrite').partitionBy(TAXID).csv(out_uniprot_folder, sep=',')
  df_pep_original.toPandas().to_csv(out_uniprot_folder + 'peptide_evidences.tsv', sep='\t', header=True, index=False)



if __name__ == '__main__':
    peptide_summary_blast_uniprot()
