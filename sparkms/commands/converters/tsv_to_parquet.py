import click
from pyspark.sql import SparkSession


@click.command('tsv-to-parquet', short_help='Command to convert tsv file to parquet')
@click.option('-i', '--input-tsv', help="Input tsv file  ie., peptides.tsv", required=True)
@click.option('-o', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def tsv_to_parquet(input_tsv, out_path):
    """
    This method read a tab delimited file and converts to parquet
    :param i: Input tsv file  ie., peptides.tsv
    :param o: Output path to store parquets. ie., /out/path
    :return:
    """

    # Read the id mapping file and keep only the Unprot accession and the Protein name
    sql_context = SparkSession.builder.getOrCreate()
    print("======= processing:" + input_tsv)
    df = sql_context.read.csv(path=input_tsv, sep='\t', header=True)
    df.show(n=30, truncate=False)

    df.write.parquet(out_path, mode='append', compression='snappy')


if __name__ == "__main__":
    tsv_to_parquet()
