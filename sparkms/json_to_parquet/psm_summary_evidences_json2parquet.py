from pyspark.sql import SQLContext
from pyspark.context import SparkContext
import glob


def main():
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    files = glob.glob("/hadoop/user/cbandla/pride/assays/*/*PrideMongoPsmSummaryEvidence.json")
    # files = glob.glob("/hadoop/user/cbandla/pride/assays/PXD002798/PXD002798_56871_PrideMongoPsmSummaryEvidence*.json")
    for f in files:
        try:
            hdfs_file = f.replace("/hadoop/user", "hdfs:///user")
            print("======= processing:" + hdfs_file)
            df = sqlContext.read.json(hdfs_file)
            if df.rdd.isEmpty():
                continue

            # compression='snappy' isn't supported in pyspark1.6. So, set at sqlContext conf
            df.write.parquet('hdfs:///user/cbandla/pride/assays_parquets/psm_summary_evidences', mode='append',
                             partitionBy=['projectAccession', 'assayAccession'])
        except Exception as e:
            print("****** SOME ERROR ****** ")
            print(e)


if __name__ == "__main__":
    main()
