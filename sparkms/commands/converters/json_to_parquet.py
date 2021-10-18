import click
from pyspark.sql import SparkSession
import glob

from sparkms.commands.analysis.peptide_summary import Fields


@click.command('json-to-parquet', short_help='Command to convert to Json files to Parquet')
@click.option('-i', '--input-path', help="Input assays folder  ie., /path/to/", required=True)
@click.option('-d', '--data-object', help="Data object to be converted (spectra, peptide, protein)", required=True)
@click.option('-o', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def json_to_parquet(input_path, data_object, out_path):
  """
  This method allows to convert a peptide json file into a parquet file for large scale data processing. The method
  allows to filter peptides by length.
  :param input_path: input path (folder) including all json files.
  :param out_path: folder where the parquet files will be generated
  :return:
  """

  pattern = "*_ArchiveSpectrum.json"
  if data_object == "peptide":
    pattern = "*_PrideMongoPeptideEvidence.json"
  elif data_object == "protein":
    pattern = "*_PrideMongoProteinEvidence.json"

  file_path = input_path + "**/" + pattern;
  sql_context = SparkSession.builder.getOrCreate()
  print("======= processing:" + file_path)
  df = sql_context.read.json(path=file_path, dropFieldIfAllNull=True)
  df.write.parquet(out_path, mode='append', partitionBy=[Fields.EXTERNAL_PROJECT_ACCESSION, Fields.ASSAY_ACCESSION],
                       compression='snappy')

if __name__ == "__main__":
  json_to_parquet()
