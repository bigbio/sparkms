import click
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


@click.command('parquet2json', short_help='Command to convert to Json files to Parquet')
@click.option('-i', '--input-path', help="Input parquet files. ie., /path/to/abc.parquet or /path/to/", required=True)
@click.option('-o', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def parquet_to_json(input_path, out_path):
    sql_context = SparkSession.builder.getOrCreate()
    df = sql_context.read.parquet(input_path)
    # df = df.withColumn("ptms_map", df["ptms_map"].cast(StringType()))
    df.write.json(out_path, mode='append',compression='gzip', ignoreNullFields=False)
    # df.show(n=10000, truncate=False)
    # print(df.count())


if __name__ == "__main__":
    parquet_to_json()

