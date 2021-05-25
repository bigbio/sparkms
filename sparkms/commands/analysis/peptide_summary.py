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


Group by:
   PeptideSequence
   proteinAccession
Properties:
   Best Search engine Score: Lowest ({"@type":"CvParam","cvLabel":"MS","accession":"MS:1002360","name":"distinct peptide-level FDRScore","value":"0.00026"})
   Best PSM: first usi in the list of that peptide
   List<ProjectAccessions>
   Number of Observations: Number of PSMs
   Map<PTMS+Position, List<PRojectAccession>>
