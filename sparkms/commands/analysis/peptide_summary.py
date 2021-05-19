import click
import os
import sys
from pyspark.sql import SparkSession, functions
from sparkms.commons.Fields import ANNOTATED_SPECTRUM_PEPTIDE_SEQUENCE, ANNOTATED_SPECTRUM_ORGANISM


@click.command('peptide-summary', short_help='Command to create peptide summary from peptide results')
@click.option('-i', '--input-path', help="Input json files. ie., /path/to/", required=True)
@click.option('-o', '--out-json', help="Output path to store parquets. ie., /out/path/peptide.json", required=True)
def peptide_summary(input_path, out_json):
  if not os.path.isdir(input_path):
    print('The output_path specified does not exist: ' + input_path)
    sys.exit(1)

  sql_context = SparkSession.builder.getOrCreate()
  df_peptides = sql_context.read.parquet(input_path)
  peptides_groups = df_peptides.groupBy(ANNOTATED_SPECTRUM_PEPTIDE_SEQUENCE, ANNOTATED_SPECTRUM_ORGANISM).



