import os
import shutil
import time
import pytest
from click.testing import CliRunner

from sparkms.commands.converters.json_to_parquet import json_to_parquet
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def runner():
    return CliRunner()


def test_psm_parquet(runner):
    input_file = 'resources/sample_jsons/'

    out_path = 'tmp' + str(round(time.time()))
    os.mkdir(out_path)
    result = runner.invoke(json_to_parquet, ['-i', input_file, '-o', out_path, '-d','spectra'])

    sql_context = SparkSession.builder.getOrCreate()
    df_spectra_original = sql_context.read.parquet(out_path)
    assert df_spectra_original.count() == 1090
    shutil.rmtree(out_path)

def test_peptide_parquet(runner):
    input_file = 'resources/sample_jsons/'

    out_path = 'tmp' + str(round(time.time()))
    os.mkdir(out_path)
    runner.invoke(json_to_parquet, ['-i', input_file, '-o', out_path, '-d','peptide'])

    sql_context = SparkSession.builder.getOrCreate()
    df_pep_original = sql_context.read.parquet(out_path)
    assert df_pep_original.count() == 2641
    shutil.rmtree(out_path)

def test_protein_parquet(runner):
    input_file = 'resources/sample_jsons/'

    out_path = 'tmp' + str(round(time.time()))
    os.mkdir(out_path)
    result = runner.invoke(json_to_parquet, ['-i', input_file, '-o', out_path, '-d','protein'])
    sql_context = SparkSession.builder.getOrCreate()
    df_protein_original = sql_context.read.parquet(out_path)
    assert df_protein_original.count() == 294
    shutil.rmtree(out_path)

