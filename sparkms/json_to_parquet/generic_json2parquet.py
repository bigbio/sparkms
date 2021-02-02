import click
from pyspark.sql import SparkSession
import glob
import sys
import os


@click.command()
@click.option('-I', '--input-path', help="Input json files. ie., /path/to/abc.json or /path/to/*", required=True)
@click.option('-O', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def main(input_path, out_path):
    if not os.path.isdir(out_path):
        print('The output_path specified does not exist: ' + out_path)
        sys.exit(1)

    sql_context = SparkSession.builder.getOrCreate()
    files = glob.glob(input_path)
    for f in files:
        try:
            print("======= processing:" + f)
            df = sql_context.read.json(f)
            if df.rdd.isEmpty():
                continue

            df.write.parquet(out_path, mode='append', partitionBy=['projectAccession', 'assayAccession'],
                             compression='snappy')
        except Exception as e:
            print("** Error while processing: " + f)
            print(e)


if __name__ == "__main__":
    main()
