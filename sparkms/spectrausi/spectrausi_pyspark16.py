from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import glob


def main():
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    spectrausi_udf = udf(lambda z: extraspectrausi(z), StringType())
    # sqlContext.udf.register("spectrausi_udf", spectrausi_udf, StringType())

    # df = sqlContext.read.json('hdfs:///user/cbandla/pride/assays/*/*ArchiveSpectrum.json')
    # df = sqlContext.read.json('hdfs:///user/cbandla/pride/assays/PXD019548/PXD019548_124301_ArchiveSpectrum.json')
    files = glob.glob("/hadoop/user/cbandla/pride/assays/*/*PrideMongoPsmSummaryEvidence.json")
    hdfs_files = []
    for f in files:
        try:
            # hdfs_files.append(f.replace("/hadoop/user", "hdfs:///user"))
            # print(hdfs_files)
            hdfs_file = f.replace("/hadoop/user", "hdfs:///user")
            print("======= processing:" + hdfs_file)
            df = sqlContext.read.json(hdfs_file)
            if df.count == 0:
                continue

            usis = df.select("usi")
            usisub = usis.withColumn('spectraUsi', spectrausi_udf('usi'))
            usisub.show(truncate=False)
            out_dir = hdfs_file[hdfs_file.rindex("/") + 1:].replace('_ArchiveSpectrum.json', '')
            usisub.write.json('hdfs:///user/cbandla/pride/spectrausis_json/' + out_dir)
        except:
            print("****** SOME ERROR ******")


def extraspectrausi(s):
    try:
        if s is None:
            return ''
        a = s.split(':')
        if len(a) < 5:
            return ''
        return ':'.join(a[:5])
    except:
        return ''


if __name__ == "__main__":
    main()
