import os
import shutil
import time
import pytest
from click.testing import CliRunner
from pyarrow import parquet
from sparkms.commands.converters.json_to_parquet import json_to_parquet


@pytest.fixture(scope="module")
def runner():
    return CliRunner()


def test_psm_parquet(runner):
    input_file = 'resources/sample_jsons/'
    reference_out_file = 'resources/sample_parquets/psm'
    out_path = 'tmp' + str(round(time.time()))
    os.mkdir(out_path)
    result = runner.invoke(json_to_parquet, ['-i', input_file, '-o', out_path, '-d "spectra"'])
    print(result)
    generated_out = parquet.read_table(out_path)
    reference_out = parquet.read_table(reference_out_file)
    shutil.rmtree(out_path)
    assert reference_out.equals(generated_out)


def test_peptide_parquet(runner):
    input_file = 'resources/sample_jsons/'
    reference_out_file = 'resources/sample_parquets/peptide'
    out_path = 'tmp' + str(round(time.time()))
    os.mkdir(out_path)
    runner.invoke(json_to_parquet, ['-i', input_file, '-o', out_path, '-d "peptide"'])
    generated_out = parquet.read_table(out_path)
    reference_out = parquet.read_table(reference_out_file)
    shutil.rmtree(out_path)
    assert reference_out.equals(generated_out)


def test_protein_parquet(runner):
    input_file = 'resources/sample_jsons/'
    reference_out_file = 'resources/sample_parquets/protein'
    out_path = 'tmp' + str(round(time.time()))
    os.mkdir(out_path)
    result = runner.invoke(json_to_parquet, ['-i', input_file, '-o', out_path, '-d "protein"'])
    print(result)
    generated_out = parquet.read_table(out_path)
    reference_out = parquet.read_table(reference_out_file)
    shutil.rmtree(out_path)
    assert reference_out.equals(generated_out)


