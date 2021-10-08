import time

import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, col, desc, first, count, expr, collect_list
from pyspark.sql.functions import sum as _sum

from sparkms.commons.Fields import PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS, \
  PROJECTS_COUNT, TAXID, ORGANISM, IS_UNIQUE_UNIPROT, PEPTIDE_COUNT, EXTERNAL_PROJECT_ACCESSIONS


@click.command('peptidome-statistics', short_help='')
@click.option('--peptide-folder', help="Input peptide summary folder in parquet files. ie., /path/to/", required=True)
@click.option('--fdr-threshold', help = 'Maximum FDR Score allowed', default = 0.01)
@click.option('--species-interest', help = 'File with species/organism of interest', required = False)
@click.option('--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def generate_peptidome_statistics(peptide_folder, fdr_threshold, species_interest, out_path):
  """
  The peptide summary to uniprot input files takes the parquet output from the peptide summary analysis pipeline
  into a uniprot output file format that can be use to push data to uniprot.

  :param peptide_folder: Folder containing all the peptides
  :param out_path: Output folder containing all the uniprot files
  :return:
  """

  # Create the Spark Context
  sql_context = SparkSession.builder.getOrCreate()
  sc = sql_context.sparkContext

  species = list()
  if species_interest is not None:
    lines = sc.textFile(species_interest)
    llist = lines.collect()
    for line in llist:
      linestrip = line.strip()
      if len(linestrip) > 0:
        species.append(linestrip)

  print(species)

  # Read the psms and peptide into a dataset
  df_pep_original = sql_context.read.parquet(peptide_folder)

  """
  Get general statistics about peptidome.
  """
  df_pep_filtered = df_pep_original.filter(df_pep_original.is_uniprot_accession == True) \
    .filter(df_pep_original.best_search_engine_score <= fdr_threshold).filter(df_pep_original.TaxId.isNotNull()) \
    .withColumn(PROJECTS_COUNT, size(df_pep_original.projectAccessions))

  """
  Get number of peptides/proteins
  """
  num_peptides = df_pep_filtered.groupBy(col(PEPTIDE_SEQUENCE), col(PROTEIN_ACCESSION)).count().select(col(PROTEIN_ACCESSION), col(PEPTIDE_SEQUENCE)).distinct().count()
  num_proteins = df_pep_filtered.select(col(PROTEIN_ACCESSION)).distinct().count()

  """
  Get the number of projects in PRIDE Archive
  """
  df_projects = df_pep_filtered.select(col(EXTERNAL_PROJECT_ACCESSIONS))
  projects = df_projects.select(collect_list(EXTERNAL_PROJECT_ACCESSIONS)).first()[0]
  all_projects = [item for sublist in projects for item in sublist]
  num_projects = len(set(all_projects))

  """
  Get the number of psms.
  """
  num_psms = df_pep_filtered.select(_sum(NUMBER_PSMS)).collect()[0][0]

  """
  Get number of unique peptides, peptides that map to only one protein
  """

  num_unique_peptides = df_pep_filtered.filter(df_pep_filtered.is_uniq_peptide_within_organism == True).groupBy(col(PEPTIDE_SEQUENCE), col(PROTEIN_ACCESSION)).count().select(
    col(PROTEIN_ACCESSION), col(PEPTIDE_SEQUENCE)).distinct().count()

  print("Projects: " + str(num_projects))
  print("Proteins: " + str(num_proteins))
  print("Peptides: " + str(num_peptides))
  print("Unique Peptides: " + str(num_unique_peptides))
  print("PSMs: " + str(num_psms))

  if len(species) > 0:
    df_pep_filtered = df_pep_filtered.where(col(ORGANISM).isin(species))

  uniprot_columns = [PEPTIDE_SEQUENCE, PROTEIN_ACCESSION, BEST_SEARCH_ENGINE, NUMBER_PSMS, PROJECTS_COUNT, TAXID, ORGANISM, IS_UNIQUE_UNIPROT]
  df_pep_original = df_pep_filtered.select(*uniprot_columns)
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
  # df_organisms.write.csv(out_statistics_folder + '/organisms-stats-' + current_time + '_tsv', sep='\t', header=True, mode='append')
  # df_peptides_per_proteins.write.csv(out_statistics_folder + '/peptides-per-proteins-stats-' + current_time + '_tsv', sep="\t", header= True, mode='append')
  # df_unique_peptides_per_proteins.write.csv(out_statistics_folder + '/unique-peptides-per-proteins-stats-' + current_time + '_tsv', sep="\t", header=True, mode='append')

  df_organisms.write.json(out_path + '/organisms-stats-' + current_time, mode='append', compression='gzip',
                          ignoreNullFields=False)
  df_peptides_per_proteins.write.json(out_path + '/peptides-per-proteins-stats-' + current_time,
                                      mode='append', compression='gzip', ignoreNullFields=False)
  df_unique_peptides_per_proteins.write.json(out_path + '/unique-peptides-per-proteins-stats-' + current_time,
                                             mode='append', compression='gzip', ignoreNullFields=False)


if __name__ == '__main__':
    generate_peptidome_statistics()
