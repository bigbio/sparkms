# Peptide information
from pandas import DataFrame
from pyspark.sql.types import *
import pandas as pd


HEADER_PEPTIDE = 'peptide sequence'
HEADER_NO_PSM = '# psms'
HEADER_PROTEIN = 'protein id' #could be array
HEADER_GENE = 'gene' # not yet
HEADER_GENE_NAME = 'gene name' # not yet

# Mass spec information
HEADER_INTENSITY = 'intensity' # not yet
HEADER_ID_SCORE = 'id score' # not yet
HEADER_MS_RUN = 'ms-run' # not yet
HEADER_MS_SCAN = 'ms-scan' # not yet
HEADER_MS_RT = "rt" # not yet
HEADER_MS_CHARGE = "charge"
HEADER_IS_DECOY = 'isdecoy'

# Sample information
HEADER_SAMPLE_ID = 'sample iD'
HEADER_PX_PROJECT_ACCESSION  = 'px accession'
HEADER_ORGANISM = 'organism'
HEADER_ORGANISM_PART = 'organism part'
HEADER_DISEASE = 'disease'
HEADER_CELL_LINE = 'cell line'

SparkPeptideTableSchema = StructType([
  StructField(HEADER_PEPTIDE, StringType(), True),
  StructField(HEADER_NO_PSM, IntegerType(), True),
  StructField(HEADER_INTENSITY, FloatType(), True),
  StructField(HEADER_IS_DECOY, IntegerType(), True),
  StructField(HEADER_PROTEIN, ArrayType(StringType()), True)
])


def mztab_to_dataframe(mztab_df: DataFrame = None) -> DataFrame:
  """
  This function converts and mzTab to a peptidetable
  :param mztab_df: MzTab DataFrame
  :return:
  """

  # Iterate over input dataframe rows and individual years
  seq_list = []
  protein_id_list = []
  for row in mztab_df.peptide_table.itertuples():
    seq_list.append(row.sequence)
    protein_id_list.append(row.accession)

  return pd.DataFrame({HEADER_PEPTIDE: seq_list,
                       HEADER_PROTEIN: protein_id_list,
  })




