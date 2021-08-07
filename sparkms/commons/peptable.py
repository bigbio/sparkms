# Peptide information

from pandas import DataFrame
from pyspark.sql.types import *

# Peptide information
HEADER_PEPTIDE = 'peptide sequence'
HEADER_PEPTIDOFORM = 'peptidoform sequence'
HEADER_MODIFICATION_STRING = 'modifications'
HEADER_NO_PSM = '# psms'
HEADER_PROTEIN = 'protein id'  # could be array
HEADER_GENE = 'gene'  # not yet
HEADER_GENE_NAME = 'gene name'  # not yet
HEADER_INTENSITY = 'intensity'  # not yet

# Mass spec information
HEADER_Q_VALUE = 'q-value'
HEADER_PEP = 'PEP'
HEADER_FEATURE_ID = 'feature id'
HEADER_ID_SCORE = 'id score'  # not yet
HEADER_MS_RUN = 'ms run'  # not yet
HEADER_MS_SPECTRA_REF = 'spectra ref'  # not yet
HEADER_MS_RT = "retention time"  # not yet
HEADER_MS_RT_WINDOW = 'retention time window'
HEADER_MS_CHARGE = "charge"
HEADER_IS_DECOY = 'isdecoy'
HEADER_MASS_TO_CHARGE = 'mass to charge'

# Sample information
HEADER_SAMPLE_ID = 'sample iD'
HEADER_PX_PROJECT_ACCESSION = 'px accession'
HEADER_ORGANISM = 'organism'
HEADER_ORGANISM_PART = 'organism part'
HEADER_DISEASE = 'disease'
HEADER_CELL_LINE = 'cell line'
HEADER_SAMPLE_CHARACTERISTICS = 'sample characteristics'

SparkPeptideTableSchema = StructType([
  StructField(HEADER_PEPTIDE, StringType(), True),
  StructField(HEADER_NO_PSM, IntegerType(), True),
  StructField(HEADER_INTENSITY, FloatType(), True),
  StructField(HEADER_IS_DECOY, IntegerType(), True),
  StructField(HEADER_PROTEIN, ArrayType(StringType()), True)
])


def change_ms_run(row, list_msruns: None):
  if row[HEADER_MS_SPECTRA_REF] == 'Cheese':
    return row['ingredient_method'][0]
  return None


def mztab_to_dataframe(mztab_df: DataFrame = None) -> DataFrame:
  """
  This function converts and mzTab to a peptidetable
  :param mztab_df: MzTab DataFrame
  :return:
  """

  result_df = mztab_df.peptide_table[['sequence', 'opt_global_cv_MS:1000889_peptidoform_sequence', 'accession',
                                      'modifications', 'opt_global_q-value',
                                      'opt_global_Posterior_Error_Probability_score',
                                      'charge', 'mass_to_charge', 'peptide_abundance_study_variable[1]',
                                      'opt_global_feature_id', 'opt_global_cv_MS:1002217_decoy_peptide',
                                      'retention_time',
                                      'retention_time_window', 'spectra_ref']].copy()

  result_df = result_df.rename(columns={"sequence": HEADER_PEPTIDE, "accession": HEADER_PROTEIN,
                                        'opt_global_cv_MS:1000889_peptidoform_sequence': HEADER_PEPTIDOFORM,
                                        'peptide_abundance_study_variable[1]': HEADER_INTENSITY,
                                        'opt_global_q-value': HEADER_Q_VALUE,
                                        'opt_global_Posterior_Error_Probability_score': HEADER_PEP,
                                        'opt_global_feature_id': HEADER_FEATURE_ID,
                                        'charge': HEADER_MS_CHARGE, 'mass_to_charge': HEADER_MASS_TO_CHARGE,
                                        'retention_time': HEADER_MS_RT, 'retention_time_window': HEADER_MS_RT_WINDOW,
                                        'opt_global_cv_MS:1002217_decoy_peptide': HEADER_IS_DECOY,
                                        'spectra_ref': HEADER_MS_SPECTRA_REF})

  list_msruns = {k: v for k, v in mztab_df.metadata.items() if ('ms_run' in k and 'location' in k)}
  result_df[HEADER_MS_SPECTRA_REF] = result_df.apply(change_ms_run, list_msruns=list_msruns)

  return result_df
