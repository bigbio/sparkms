import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, col, desc, first, count, avg, mean, expr
import time

from sparkms.commons.Fields import PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS, \
  PROJECTS_COUNT, TAXID, ORGANISM, IS_UNIQUE_UNIPROT, PEPTIDE_COUNT


@click.command('peptidome-statistics', short_help='')
@click.option('--peptide-folder', help="Input peptide summary folder in parquet files. ie., /path/to/", required=True)
@click.option('--fdr-threshold', help = 'Maximum FDR Score allowed', default = 0.01)
@click.option('--species-interest', help = 'File with species/organism of interest', required = False)
@click.option('--out-statistics-folder', help="Output path to store parquets. ie., /out/path", required=True)
def generate_peptidome_statistics(peptide_folder, fdr_threshold, species_interest, out_statistics_folder):
  """
  The peptide summary to uniprot input files takes the parquet output from the peptide summary analysis pipeline
  into a uniprot output file format that can be use to push data to uniprot.

  :param peptide_folder: Folder containing all the peptides
  :param out_statistics_folder: Output folder containing all the uniprot files
  :return:
  """

  # Create the Spark Context
  sql_context = SparkSession.builder.getOrCreate()

  species = list()
  if species_interest is not None:
    with open(species_interest, "r") as reader_species:
      species = [i.strip() for i in reader_species.readlines()]

  # Read the psms and peptide into a dataset
  df_pep_original = sql_context.read.parquet(peptide_folder)

  if len(species) > 0:
    df_pep_original = df_pep_original.where(col(ORGANISM).isin(species))

  df_pep_original = df_pep_original.filter(df_pep_original.is_uniprot_accession == True)\
    .filter(df_pep_original.best_search_engine_score <= fdr_threshold).filter(df_pep_original.TaxId.isNotNull())\
    .withColumn(PROJECTS_COUNT, size(df_pep_original.projectAccessions))

  uniprot_columns = [PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS, PROJECTS_COUNT, TAXID, ORGANISM, IS_UNIQUE_UNIPROT]
  df_pep_original = df_pep_original.select(*uniprot_columns)
  df_pep_original.show(truncate=False, n=300)

  """
  Get the number of peptides per organism. The list is sorted by number of proteins.
  """
  df_organisms = df_pep_original.groupBy(col(ORGANISM)).count().sort(desc("count"))
  df_organisms.show(truncate=False, n=300)

  """
  Average number of peptides per protein by each species.
  """
  df_peptides_per_proteins = df_pep_original.groupBy(col(PROTEIN_ACCESSION))\
    .agg(count(PROTEIN_ACCESSION).alias(PEPTIDE_COUNT),first(ORGANISM).alias(ORGANISM))
  df_peptides_per_proteins = df_peptides_per_proteins.groupBy(col(ORGANISM)).agg(expr('percentile(peptide_count, array(0.5))')[0].alias(PEPTIDE_COUNT)).sort(desc(PEPTIDE_COUNT))
  df_peptides_per_proteins.show(n = 300)

  """
  Average number of unique peptides per protein by each species.
  """
  df_unique_peptides_per_proteins = df_pep_original.filter(df_pep_original.is_uniq_peptide_within_organism == True)\
    .groupBy(col(PROTEIN_ACCESSION)) \
    .agg(count(PROTEIN_ACCESSION).alias(PEPTIDE_COUNT), first(ORGANISM).alias(ORGANISM))

  df_unique_peptides_per_proteins = df_unique_peptides_per_proteins.groupBy(col(ORGANISM)).agg(expr('percentile(peptide_count, array(0.5))')[0].alias(PEPTIDE_COUNT)).sort(desc(PEPTIDE_COUNT))
  df_unique_peptides_per_proteins.show(n=300)

  current_time = time.strftime("%Y-%m")
  df_organisms.toPandas().to_csv(out_statistics_folder + '/organisms-stats-' + current_time + '.tsv', sep='\t', header=True, index=False)
  df_peptides_per_proteins.toPandas().to_csv(out_statistics_folder + '/peptides-per-proteins-stats-' + current_time + '.tsv', sep="\t", header= True, index=False)
  df_unique_peptides_per_proteins.toPandas().to_csv(out_statistics_folder + '/unique-peptides-per-proteins-stats-' + current_time + '.tsv', sep="\t", header=True, index=False)

if __name__ == '__main__':
    generate_peptidome_statistics()
