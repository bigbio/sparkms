import os
import shutil
import time
import pytest
from click.testing import CliRunner
from pyarrow import parquet
from sparkms.json_to_parquet.generic_json2parquet import main


@pytest.fixture(scope="module")
def runner():
    return CliRunner()


def test_psm_parquet(runner):
    input_file = 'resources/sample_input/PXD002681_56166_PrideMongoPsmSummaryEvidence.json'
    reference_out_file = 'resources/sample_output/psm'
    out_path = 'temp' + str(round(time.time()))
    os.mkdir(out_path)
    runner.invoke(main, ['-I', input_file, '-O', out_path])
    generated_out = parquet.read_table(out_path)
    reference_out = parquet.read_table(reference_out_file)
    shutil.rmtree(out_path)
    assert reference_out.equals(generated_out)


def test_peptide_parquet(runner):
    input_file = 'resources/sample_input/PXD002681_56166_PrideMongoPeptideEvidence.json'
    reference_out_file = 'resources/sample_output/peptide'
    out_path = 'temp' + str(round(time.time()))
    os.mkdir(out_path)
    runner.invoke(main, ['-I', input_file, '-O', out_path])
    generated_out = parquet.read_table(out_path)
    reference_out = parquet.read_table(reference_out_file)
    shutil.rmtree(out_path)
    assert reference_out.equals(generated_out)


def test_protein_parquet(runner):
    input_file = 'resources/sample_input/PXD002681_56166_PrideMongoProteinEvidence.json'
    reference_out_file = 'resources/sample_output/protein'
    out_path = 'temp' + str(round(time.time()))
    os.mkdir(out_path)
    runner.invoke(main, ['-I', input_file, '-O', out_path])
    generated_out = parquet.read_table(out_path)
    reference_out = parquet.read_table(reference_out_file)
    shutil.rmtree(out_path)
    assert reference_out.equals(generated_out)


