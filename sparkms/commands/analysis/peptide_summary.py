import os
import sys
import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import size
from pyspark.sql.functions import struct
from pyspark.sql.functions import length
from pyspark.sql.functions import map_from_entries

from typing import Final

from pyspark.sql.types import StringType


class Fields:
    ID: Final = "id"
    ACCESSION: Final = "accession"
    ASSAY: Final = "assayType"
    PX_ACCESSION: Final = "pxAccession"
    PROJECT_TILE: Final = "title"
    FILE_RELATIONS_IN_PROJECT: Final = "submittedFileRelations"
    ACCESSION_SUBMISSION_FILE: Final = "accessionSubmissionFile"
    ADDITIONAL_ATTRIBUTES: Final = "additionalAttributes"
    PROJECT_DESCRIPTION: Final = "description"
    SAMPLE: Final = "sample"
    SAMPLE_FILE_CHECKSUM: Final = "filechecksum"
    PROJECT_SAMPLE_PROTOCOL: Final = "sampleProtocol"
    PROJECT_DATA_PROTOCOL: Final = "dataProtocol"
    PROJECT_TAGS: Final = "tags"
    PROJECT_KEYWORDS: Final = "keywords"
    PROJECT_DOI: Final = "projectDoi"
    PROJECT_OMICS_LINKS: Final = "project_other_omics"
    PROJECT_SUBMISSION_TYPE: Final = "submissionType"
    SUBMISSION_DATE: Final = "submissionDate"
    PUBLICATION_DATE: Final = "publicationDate"
    UPDATED_DATE: Final = "updatedDate"
    PROJECT_SUBMITTER: Final = "submitters"
    PROJECT_PI_NAMES: Final = "lab_heads"
    INSTRUMENTS: Final = "instruments"
    SOFTWARES: Final = "softwares"
    QUANTIFICATION_METHODS: Final = "quantificationMethods"
    COUNTRIES: Final = "countries"
    SAMPLE_ATTRIBUTES_NAMES: Final = "sample_attributes"
    PROJECT_REFERENCES: Final = "project_references"
    PROJECT_IDENTIFIED_PTM: Final = "ptmList"
    PRIDE_PROJECTS_COLLECTION_NAME: Final = "pride_projects"
    PRIDE_EXPERIMENTAL_DESIGN_COLLECTION_NAME: Final = "pride_experimental_design"
    PRIDE_FILE_COLLECTION_NAME: Final = "pride_files"
    PRIDE_MSRUN_COLLECTION_NAME: Final = "pride_msruns"
    PRIDE_REANALYSIS_COLLECTION_NAME: Final = "pride_reanalysis_collection"
    PRIDE_MOLECULES_COLLECTION_NAME: Final = "pride_molecules"
    PRIDE_SDRF_COLLECTION_NAME: Final = "pride_sdrf"
    PUBLIC_PROJECT: Final = "public_project"
    EXPERIMENTAL_FACTORS: Final = "experimentalFactors"
    EXTERNAL_PROJECT_ACCESSIONS: Final = "projectAccessions"
    EXTERNAL_ANALYSIS_ACCESSIONS: Final = "analysisAccessions"
    FILE_CATEGORY: Final = "fileCategory"
    FILE_SOURCE_TYPE: Final = "fileSourceType"
    FILE_SOURCE_FOLDER: Final = "fileSourceFolder"
    FILE_CHECKSUM: Final = "fileChecksum"
    CHECKSUM: Final = "checksum"
    SUBMITTER_FILE_CHECKSUM: Final = "submitterFileChecksum"
    FILE_PUBLIC_LOCATIONS: Final = "filePublicLocations"
    FILE_SIZE_MB: Final = "fileSizeMB"
    FILE_EXTENSION: Final = "fileExtension"
    FILE_NAME: Final = "fileName"
    FILE_IS_COMPRESS: Final = "fileCompress"
    PRIDE_PEPTIDE_COLLECTION_NAME: Final = "pride_peptide_evidences"
    PRIDE_PSM_COLLECTION_NAME: Final = "pride_psm_evidences"
    PSM_SPECTRUM_ACCESSIONS: Final = "psmAccessions"
    PEPTIDE_SEQUENCE: Final = "peptideSequence"
    MODIFIED_PEPTIDE_SEQUENCE: Final = "modifiedPeptideSequence"
    REPORTED_PROTEIN_ACCESSION: Final = "reportedProteinAccession"
    REPORTED_FILE_ID: Final = "reportedFileID"
    EXTERNAL_PROJECT_ACCESSION: Final = "projectAccession"
    EXTERNAL_ANALYSIS_ACCESSION: Final = "analysisAccession"
    PROTEIN_ASSAY_ACCESSION: Final = "assayAccession"
    IDENTIFICATION_DATABASE: Final = "database"
    PEPTIDE_UNIQUE: Final = "peptideUnique"
    BEST_PSM_SCORE: Final = "bestPSMScore"
    RETENTION_TIME: Final = "retentionTime"
    CHARGE: Final = "charge"
    PRECURSOR_MASS: Final = "precursorMass"
    EXPERIMENTAL_MASS_TO_CHARGE: Final = "expMassToCharge"
    CALCULATED_MASS_TO_CHARGE: Final = "calculatedMassToCharge"
    PRE_AMINO_ACID: Final = "preAminoAcid"
    POST_AMINO_ACID: Final = "postAminoAcid"
    START_POSITION: Final = "startPosition"
    END_POSITION: Final = "endPosition"
    SEARCH_ENGINE_SCORES: Final = "searchEngineScores"
    MISSED_CLEAVAGES: Final = "missedCleavages"
    PRIDE_ANALYSIS_COLLECTION: Final = "pride_analysis_collection"
    PRIDE_STATS_COLLECTION: Final = "pride_stats_collection"
    STATS_SUBMISSION_COUNTS: Final = "pride_submission_counts"
    STATS_ESTIMATION_DATE: Final = "estimationDate"
    STATS_COMPLEX_COUNTS: Final = "pride_complex_counts"
    MS_RUN_FILE_PROPERTIES: Final = "FileProperties"
    MS_RUN_INSTRUMENT_PROPERTIES: Final = "InstrumentProperties"
    MS_RUN_MS_DATA: Final = "MsData"
    MS_RUN_SCAN_SETTINGS: Final = "ScanSettings"
    MS_RUN_ID_SETTINGS: Final = "IdSettings"
    MS_RUN_ID_SETTINGS_FIXED_MODIFICATIONS: Final = "FixedModifications"
    MS_RUN_ID_SETTINGS_VARIABLE_MODIFICATIONS: Final = "VariableModifications"
    MS_RUN_ID_SETTINGS_ENZYMES: Final = "Enzymes"
    MS_RUN_ID_SETTINGS_FRAGMENT_TOLERANCE: Final = "FragmentTolerance"
    MS_RUN_ID_SETTINGS_PARENT_TOLERANCE: Final = "ParentTolerance"
    MONGO_MSRUN_DOCUMENT_ALIAS: Final = "MongoPrideMSRun"
    MONGO_FILE_DOCUMENT_ALIAS: Final = "MongoPrideFile"
    SAMPLES: Final = "samples"
    SAMPLES_MSRUN: Final = "samples_msrun"
    PRIDE_ASSAY_COLLECTION_NAME: Final = "pride_assays"
    ASSAY_ACCESSION: Final = "assayAccession"
    ASSAY_FILE_NAME: Final = "fileName"
    ASSAY_TITLE: Final = "assayTitle"
    ASSAY_DESCRIPTION: Final = "assayDescription"
    ASSAY_DATA_ANALYSIS_SOFTWARE: Final = "dataAnalysisSoftwares"
    ASSAY_DATA_ANALYSIS_DATABASE: Final = "dataAnalysisDatabase"
    ASSAY_DATA_ANALYSIS_RESULTS: Final = "summaryResults"
    ASSAY_DATA_ANALYSIS_PROTOCOL: Final = "dataAnalysisProperties"
    ASSAY_DATA_ANALYSIS_PTMS: Final = "ptmsResults"
    ASSAY_FILES: Final = "assayFiles"
    VALID_ASSAY: Final = "validAssay"
    PRIDE_PROTEIN_COLLECTION_NAME: Final = "pride_protein_evidences"
    PROTEIN_SEQUENCE: Final = "proteinSequence"
    UNIPROT_MAPPED_PROTEIN_ACCESSION: Final = "uniprotMappedProteinAccession"
    ENSEMBL_MAPPED_PROTEIN_ACCESSION: Final = "ensemblMappedProteinAccession"
    PROTEIN_GROUP_MEMBERS: Final = "proteinGroupMembers"
    PROTEIN_DESCRIPTION: Final = "proteinDescription"
    PROTEIN_MODIFICATIONS: Final = "ptms"
    IS_DECOY: Final = "isDecoy"
    BEST_SEARCH_ENGINE: Final = "bestSearchEngineScore"
    PROTEIN_REPORTED_ACCESSION: Final = "reportedAccession"
    MSRUN_PROPERTIES: Final = "MSRunProperties"
    PEPTIDE_ACCESSION: Final = "peptideAccession"
    PROTEIN_ACCESSION: Final = "proteinAccession"
    PEPTIDE_ACCESSIONS: Final = "peptideAccessions"
    PROTEIN_ACCESSIONS: Final = "proteinAccessions"
    QUALITY_ESTIMATION_METHOD: Final = "qualityEstimationMethods"
    IS_VALIDATED: Final = "isValid"
    VALUE: Final = "value"
    NUMBER_PEPTIDEEVIDENCES: Final = "numberPeptides"
    NUMBER_PSMS: Final = "numberPSMs"
    PROTEIN_COVERAGE: Final = "sequenceCoverage"
    USI: Final = "usi"
    SPECTRA_USI: Final = "spectraUsi"
    PSM_SUMMARY_FILE: Final = "fileName"

@click.command('peptide_summary', short_help='')
@click.option('-psm', help="Input psm parquet files. ie., /path/to/", required=True)
@click.option('-pep', help="Input peptide parquet files. ie., /path/to/", required=True)
@click.option('-o', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def peptide_summary(psm, pep, out_path):

    sql_context = SparkSession.builder.getOrCreate()
    df_psm_original = sql_context.read.parquet(psm)
    df_pep_original = sql_context.read.parquet(pep)

    #filter out smaller peptides
    df_pep = df_pep_original.filter(length(Fields.PEPTIDE_SEQUENCE) > 7)
    df_psm = df_psm_original.filter(length(Fields.PEPTIDE_SEQUENCE) > 7)
    # df_pep.show(truncate=False)
    # df_psm.show(truncate=False)

    df_psm_explode_additional_attr = df_psm.select(Fields.USI,
                                                   explode(Fields.ADDITIONAL_ATTRIBUTES).alias(
                                                       Fields.ADDITIONAL_ATTRIBUTES))
    df_psm_fdr = df_psm_explode_additional_attr.filter("additionalAttributes.accession == 'MS:1002355'") \
        .select(Fields.USI, col('additionalAttributes.value').alias('fdrscore'))
    # df_psm_fdr.show(truncate=False)

    df_psm_fdr = df_psm_fdr.filter(col('fdrscore') > 0.0)
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

    df_pep_fdr = df_pep_fdr.filter(col('fdrscore') > 0.0)
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

    df_pep_ptm3 = df_pep_ptm3.withColumn('ptms', df_pep_ptm3['ptms'].cast(StringType()))

    df_pep_ptm4 = df_pep_ptm3.groupby(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION) \
        .agg(functions.collect_set(struct('ptms','project_accessions')))\
        .toDF(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, 'ptms_project_accessions')\
        .select(Fields.PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, map_from_entries('ptms_project_accessions').alias('ptms_map'))
    # df_pep_ptm4.show(truncate=False)
    # df_pep_ptm4.printSchema()

    df_pep_summary4 = df_pep_summary3.join(df_pep_ptm4,
                                           (df_pep_summary3.peptideSequence == df_pep_ptm4.peptideSequence) &
                                           (df_pep_summary3.proteinAccession == df_pep_ptm4.proteinAccession)) \
        .select(df_pep_summary3.peptideSequence, df_pep_summary3.proteinAccession, Fields.EXTERNAL_PROJECT_ACCESSIONS,
                'best_search_engine_score', 'psms_count', 'best_usis', 'ptms_map')

    df_pep_summary4.write.parquet(out_path, mode='append', compression='snappy')
    # df_pep_summary4.show(truncate=False)


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