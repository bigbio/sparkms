import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, length
from pyspark.sql.functions import col

@click.command('uniprot-mapping-to-parquet', short_help='Command to convert id mapping file to parquet')
@click.option('-i', '--input-path', help="Input uniprot mapping tab file  ie., idmapping_selected.tab.2015_03.gz", required=True)
@click.option('-o', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def uniprot_mapping_to_parquet(input_path, out_path):
  """
  This method read a tab delimitted idmapping file from uniprot and generate a parquet file
  :param input_path: input path (folder) including all json files.
  :param out_path: folder where the parquet files will be generated
  :return:
  """

  sql_context = SparkSession.builder.getOrCreate()
  print("======= processing:" + input_path)
  df = sql_context.read.csv(path=input_path, sep='\t', header=False)
  df = df.select(df.columns[:2]).toDF("AC", "ID")
  df_uniprot = df.select([trim(col(c)).alias(c) for c in df.columns])
  # df2 = df_uniprot.select("AC", "ID", length("ID"))
  df_uniprot.show(n = 30)
  df_uniprot.write.parquet(out_path, mode='append', compression='snappy')

if __name__ == "__main__":
  uniprot_mapping_to_parquet()
