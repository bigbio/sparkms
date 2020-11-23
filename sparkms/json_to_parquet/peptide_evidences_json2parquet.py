from pyspark.sql import SQLContext
from pyspark.context import SparkContext
import glob
import argparse
import sys
import os

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help="Input josn files. ie., /path/to/abc.json or  /path/to/*", required=True)
    parser.add_argument("-o", "--output", help="Output path to store parquets. ie., /out/path", required=True)
    args = parser.parse_args()
    input_path = args.input
    out_path = args.output

    if not os.path.isdir(out_path):
        print('The output_path specified does not exist')
        sys.exit(1)

    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    files = glob.glob(input_path)
    for f in files:
        try:
            print("======= processing:" + f)
            df = sqlContext.read.json(f)
            if df.rdd.isEmpty():
                continue

            df.write.parquet(out_path + '/peptides', mode='append', partitionBy=['projectAccession', 'assayAccession'])
        except Exception as e:
            print("** Error while processing: " + f)
            print(e)


if __name__ == "__main__":
    main()
