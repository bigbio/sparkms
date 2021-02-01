from pyspark.sql import SparkSession
import glob
import argparse
import sys
import os


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-I', '--input', help="Input josn files. ie., /path/to/abc.json or  /path/to/*", required=True)
    parser.add_argument("-O", "--output", help="Output path to store parquets. ie., /out/path", required=True)
    args = parser.parse_args()
    input_path = args.input
    out_path = args.output
    return input_path, out_path


def main():
    input_path, out_path = parse_args()
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
