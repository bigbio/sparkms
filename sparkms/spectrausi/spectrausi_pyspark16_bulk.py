from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


def main():
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    spectrausi_udf = udf(lambda z: extraspectrausi(z), StringType())
    # sqlContext.udf.register("spectrausi_udf", spectrausi_udf, StringType())

    df = sqlContext.read.json('hdfs:///user/cbandla/pride/assays/*/*PrideMongoPsmSummaryEvidence.json')
    # df = sqlContext.read.json('hdfs:///user/cbandla/pride/assays/PXD019548/PXD019548_124301_ArchiveSpectrum.json')
    try:
        usis = df.select("usi")
        usisub = usis.withColumn('spectraUsi', spectrausi_udf('usi'))
        usisub.show(truncate=False)
        usisub.write.json('hdfs:///user/cbandla/pride/spectrausis_json_bulk/')
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
