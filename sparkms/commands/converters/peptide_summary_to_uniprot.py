import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import size, col, desc

from sparkms.commons.Fields import PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS,\
  PROJECTS_COUNT, TAXID

@click.command('peptide-summary-to-uniprot', short_help='')
@click.option('--peptide-folder', help="Input peptide summary folder in parquet files. ie., /path/to/", required=True)
@click.option('--fdr-threshold', help = 'Maximum FDR Score allowe', default = 0.01)
@click.option('--out-uniprot-folder', help="Output path to store parquets. ie., /out/path", required=True)
def peptide_summary_to_uniprot(peptide_folder, fdr_threshold, out_uniprot_folder):
  """
  The peptide summary to uniprot input files takes the parquet output from the peptide summary analysis pipeline
  into a uniprot output file format that can be use to push data to uniprot.

  :param peptide_folder: Folder containing all the peptides
  :param out_uniprot_folder: Output folder containing all the uniprot files
  :return:
  """

  # Create the Spark Context
  sql_context = SparkSession.builder.getOrCreate()

  # Read the psms and peptide into a dataset
  df_pep_original = sql_context.read.parquet(peptide_folder)

  df_pep_original = df_pep_original.filter(df_pep_original.is_uniprot_accession == True)\
    .filter(df_pep_original.best_search_engine_score <= fdr_threshold).filter(df_pep_original.TaxId.isNotNull())\
    .withColumn(PROJECTS_COUNT, size(df_pep_original.projectAccessions))

  uniprot_columns = [PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS, PROJECTS_COUNT, TAXID]
  df_pep_original = df_pep_original.select(*uniprot_columns)
  df_pep_original.show(truncate=False, n=300)

  df_pep_original.toPandas().to_csv(out_uniprot_folder + 'peptide_evidences.tsv', sep='\t', header=True, index=False)


if __name__ == '__main__':
    peptide_summary_to_uniprot()
