import os
import shutil
import time

from pyarrow import parquet

from sparkms.json_to_parquet.generic_json2parquet import main


def test_psm_parquet(mocker):
    input_file = 'resources/sample_input/PXD002681_56166_PrideMongoPsmSummaryEvidence.json'
    reference_out_file = 'resources/sample_output/psm'
    out_path = 'temp' + str(round(time.time()))
    os.mkdir(out_path)
    ret = (input_file, out_path)
    mocker.patch('sparkms.json_to_parquet.generic_json2parquet.parse_args', return_value=ret)
    main()
    generated_out = parquet.read_table(out_path)
    reference_out = parquet.read_table(reference_out_file)
    shutil.rmtree(out_path)
    assert reference_out.equals(generated_out)

def test_peptide_parquet(mocker):
    input_file = 'resources/sample_input/PXD002681_56166_PrideMongoPeptideEvidence.json'
    reference_out_file = 'resources/sample_output/peptide'
    out_path = 'temp' + str(round(time.time()))
    os.mkdir(out_path)
    ret = (input_file, out_path)
    mocker.patch('sparkms.json_to_parquet.generic_json2parquet.parse_args', return_value=ret)
    main()
    generated_out = parquet.read_table(out_path)
    reference_out = parquet.read_table(reference_out_file)
    shutil.rmtree(out_path)
    assert reference_out.equals(generated_out)

def test_protein_parquet(mocker):
    input_file = 'resources/sample_input/PXD002681_56166_PrideMongoProteinEvidence.json'
    reference_out_file = 'resources/sample_output/protein'
    out_path = 'temp' + str(round(time.time()))
    os.mkdir(out_path)
    ret = (input_file, out_path)
    mocker.patch('sparkms.json_to_parquet.generic_json2parquet.parse_args', return_value=ret)
    main()
    generated_out = parquet.read_table(out_path)
    reference_out = parquet.read_table(reference_out_file)
    shutil.rmtree(out_path)
    assert reference_out.equals(generated_out)


