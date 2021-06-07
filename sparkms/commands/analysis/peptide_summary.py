import os
import sys
import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import size
from pyspark.sql.functions import struct
from pyspark.sql.functions import map_from_entries
from sparkms.commons import Fields


@click.command('peptide_summary', short_help='')
@click.option('-psm', help="Input psm parquet files. ie., /path/to/", required=True)
@click.option('-pep', help="Input peptide parquet files. ie., /path/to/", required=True)
@click.option('-O', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def peptide_summary(psm, pep, out_path):
    if not os.path.isdir(out_path):
        print('The output_path specified does not exist: ' + out_path)
        sys.exit(1)

    sql_context = SparkSession.builder.getOrCreate()
    df_psm = sql_context.read.parquet(psm)
    df_pep = sql_context.read.parquet(pep)

    df_psm_explode_additional_attr = df_psm.select(Fields.USI,
                                                   explode(Fields.ADDITIONAL_ATTRIBUTES).alias(
                                                       Fields.ADDITIONAL_ATTRIBUTES))
    df_psm_fdr = df_psm_explode_additional_attr.filter("additionalAttributes.accession == 'MS:1002355'") \
        .select(Fields.USI, col('additionalAttributes.value').alias('fdrscore'))
    # df_psm_fdr.show(truncate=False)

    df_pep_psm = df_pep.select(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION,
                               explode(Fields.PSM_SPECTRUM_ACCESSIONS).alias("psm"))
    # df_pep_psm.show(truncate=False)

    df_pep_psm_count = df_pep_psm.groupby(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION) \
        .agg(functions.collect_set('psm.usi').alias('usis')) \
        .select(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, size('usis').alias('psms_count'))
    # df_pep_psm_count.show(truncate=False)

    df_pep_usi = df_pep_psm.join(df_psm_fdr, (df_psm_fdr.usi == df_pep_psm['psm.usi'])) \
        .groupby(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, 'fdrscore') \
        .agg(functions.collect_set(Fields.USI)) \
        .toDF(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, 'fdrscore', 'usis')
    # df_pep_usi.filter("proteinAccession == 'Q5XI78'").show(truncate=False)

    df_pep_usi_best_fdr = df_pep_usi.groupby(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION) \
        .agg(functions.min('fdrscore')).toDF(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, "min_fdrscore")
    # df_pep_usi_best_fdr.filter("proteinAccession == 'Q5XI78'").show(truncate=False)

    df_pep_best_usis = df_pep_usi_best_fdr.join(df_pep_usi,
                                                (df_pep_usi.peptideSequence == df_pep_usi_best_fdr.peptideSequence) &
                                                (df_pep_usi.proteinAccession == df_pep_usi_best_fdr.proteinAccession) &
                                                (df_pep_usi.fdrscore == df_pep_usi_best_fdr.min_fdrscore)) \
        .select(df_pep_usi.peptideSequence, df_pep_usi.proteinAccession, 'usis')
    # df_pep_best_usis.show(truncate=False)

    df_pep_explode_additional_attr = df_pep.select(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION,
                                                   Fields.EXTERNAL_PROJECT_ACCESSION,
                                                   explode(Fields.ADDITIONAL_ATTRIBUTES).alias(
                                                       Fields.ADDITIONAL_ATTRIBUTES))
    # df_pep_explode_additional_attr.show(truncate=False)

    df_pep_fdr = df_pep_explode_additional_attr.filter("additionalAttributes.accession == 'MS:1002360'") \
        .select(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, Fields.EXTERNAL_PROJECT_ACCESSION,
                col('additionalAttributes.value').alias('fdrscore'))
    # df_pep_fdr.show(truncate=False)

    df_pep_summary1 = df_pep_psm.join(df_pep_fdr, (df_pep_psm.peptideSequence == df_pep_fdr.peptideSequence) &
                                      (df_pep_psm.proteinAccession == df_pep_fdr.proteinAccession)) \
        .select(df_pep_psm.peptideSequence, df_pep_psm.proteinAccession,
                Fields.EXTERNAL_PROJECT_ACCESSION, 'fdrscore') \
        .groupby(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION) \
        .agg(functions.collect_set(Fields.EXTERNAL_PROJECT_ACCESSION), functions.min('fdrscore')) \
        .toDF(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, Fields.EXTERNAL_PROJECT_ACCESSIONS,
              'best_search_engine_score')
    # df_pep_summary1.show(truncate=False)

    df_pep_summary2 = df_pep_summary1.join(df_pep_psm_count,
                                           (df_pep_summary1.peptideSequence == df_pep_psm_count.peptideSequence) &
                                           (df_pep_summary1.proteinAccession == df_pep_psm_count.proteinAccession)) \
        .select(df_pep_summary1.peptideSequence, df_pep_summary1.proteinAccession, Fields.EXTERNAL_PROJECT_ACCESSIONS,
                'best_search_engine_score', 'psms_count')
    # df_pep_summary2.show(truncate=False)

    df_pep_summary3 = df_pep_summary2.join(df_pep_best_usis,
                                           (df_pep_summary2.peptideSequence == df_pep_best_usis.peptideSequence) &
                                           (df_pep_summary2.proteinAccession == df_pep_best_usis.proteinAccession)) \
        .select(df_pep_summary2.peptideSequence, df_pep_summary2.proteinAccession, Fields.EXTERNAL_PROJECT_ACCESSIONS,
                'best_search_engine_score', 'psms_count', col('usis').alias('best_usis'))
    # df_pep_summary3.show(truncate=False)

    df_pep_ptm = df_pep.select(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, Fields.EXTERNAL_PROJECT_ACCESSION,
                               explode(Fields.PROJECT_IDENTIFIED_PTM).alias("ptms"))
    df_pep_ptm2 = df_pep_ptm.select(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION,
                                    Fields.EXTERNAL_PROJECT_ACCESSION,
                                    col('ptms.modification.accession').alias('modification'),
                                    explode("ptms.positionMap").alias("positionMap"))
    df_pep_ptm3 = df_pep_ptm2.groupby(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, struct('modification',
                                    'positionMap.key'))\
        .agg(functions.collect_set(Fields.EXTERNAL_PROJECT_ACCESSION))\
        .toDF(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, 'ptms', 'project_accessions')
    # df_pep_ptm3.show(truncate=False)

    df_pep_ptm4 = df_pep_ptm3.groupby(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION) \
        .agg(functions.collect_set(struct('ptms','project_accessions')))\
        .toDF(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, 'ptms_project_accessions')\
        .select(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, map_from_entries('ptms_project_accessions').alias('ptms_map'))
    df_pep_ptm4.show(truncate=False)
    # df_pep_ptm4.printSchema()

    df_pep_summary4 = df_pep_summary3.join(df_pep_ptm4,
                                           (df_pep_summary3.peptideSequence == df_pep_ptm4.peptideSequence) &
                                           (df_pep_summary3.proteinAccession == df_pep_ptm4.proteinAccession)) \
        .select(df_pep_summary3.peptideSequence, df_pep_summary3.proteinAccession, Fields.EXTERNAL_PROJECT_ACCESSIONS,
                'best_search_engine_score', 'psms_count', 'best_usis', 'ptms_map')
    df_pep_summary4.show(truncate=False)

    df_pep_summary4.write.parquet(out_path, mode='append', compression='snappy')


if __name__ == '__main__':
    peptide_summary()

# Group by:
#    PeptideSequence
#    proteinAccession
# Properties:
#    Best Search engine Score: Lowest ({"@type":"CvParam","cvLabel":"MS","accession":"MS:1002360","name":"distinct peptide-level FDRScore","value":"0.00026"})
#    Best PSM: first usi in the list of that peptide
#    List<ProjectAccessions>
#    Number of Observations: Number of PSMs
#    Map<PTMS+Position, List<PRojectAccession>>