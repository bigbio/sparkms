from functools import reduce

import click
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import trim, length
from pyspark.sql.functions import col
from pyspark.sql.functions import lower
import pyspark.sql.functions as f

from sparkms.commons.uniprot_functions import get_protein_name_from_header, get_info_from_header, get_PE


def uniprot_header_to_df(row):
    """
    This function will read a row in a format of a fasta header and retrieve a list of properties of the protein.
    At the moment the following properties are captured:
    - Protein accession
    - Organism
    - Gene name
    The structure of the header should be:
    >sp|P0C9T5|5054R_ASFWA Protein MGF 505-4R OS=African swine fever virus (isolate Warthog/Namibia/Wart80/1980) OX=561444 GN=War-038 PE=3 SV=1'

    :param uniprot_header: Uniprot fasta header
    :return: List of properties
    """
    result = list()
    uniprot_header = row[0]
    accession = uniprot_header.split("|")[1]
    result.append(accession)

    protein_name = get_protein_name_from_header(uniprot_header)
    result.append(protein_name)

    gene_name = get_info_from_header(uniprot_header, "GN")
    result.append(gene_name)

    organism = get_info_from_header(uniprot_header, "OS")
    result.append(organism)

    taxid = get_info_from_header(uniprot_header, "OX")
    result.append(taxid)

    pe = get_PE(uniprot_header)
    result.append(pe)

    return result


@click.command('uniprot-mapping-to-parquet', short_help='Command to convert id mapping file to parquet')
@click.option('-i', '--input-id-mapping', help="Input uniprot mapping tab file  ie., idmapping_selected.tab.2015_03.gz",
              required=True)
@click.option('-s', '--uniprot-fasta-folder', help="Uniprot fasta folder. i.e. /input/fasta-files/ ", required=True)
@click.option('-o', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def uniprot_mapping_to_parquet(input_id_mapping, uniprot_fasta_folder, out_path):
    """
    This method read a tab delimited idmapping file from uniprot and a set of FASTA files from a directory to
    generate a parquet file with the following five columns:

    - AC: Uniprot Protein Accession
    - ID: Uniprot Protein Name
    - Gene: Gene name for the corresponding protein
    - Organism: Organism for the corresponding protein
    - EvidenceLevel: Protein evidence level following uniprot guidelines


    :param input_id_mapping: input id tab-delimited mapping file from uniprot
    :param uniprot_fasta_folder: folder containing fasta files from uniprot (ext fasta.gz)
    :return:
    """

    # Read the id mapping file and keep only the Unprot accession and the Protein name
    sql_context = SparkSession.builder.getOrCreate()
    print("======= processing:" + input_id_mapping)
    df = sql_context.read.csv(path=input_id_mapping, sep='\t', header=False)
    df = df.select(df.columns[:21]).toDF("AC", "ID","GeneID","RefSeq","GI","PDB","GO","UniRef100","UniRef90","UniRef50","UniParc","PIR","NCBI-taxon","MIM","UniGene",
                                        "PubMed","EMBL","EMBL-CDS","Ensembl","Ensembl_TRS","Ensembl_PRO")

    df_uniprot_id= df.select(col("AC"),col("ID"))
    df_uniprot_id.show(n=30, truncate=False)
    df_ref_seq   = df.select(col("AC"), col("RefSeq")).withColumnRenamed("RefSeq", "ID")
    df_ref_seq.show(n=30, truncate=False)
    df_ensembl_seq = df.select(col("AC"), col("Ensembl_PRO"))
    df_ensembl_seq = df_ensembl_seq.select("AC", f.split("Ensembl_PRO", "; ").alias("letters"),
                          f.posexplode(f.split("Ensembl_PRO", "; ")).alias("pos", "val")).select("AC","val")
    df_ensembl_seq = df_ensembl_seq.withColumnRenamed("val", "ID")
    # df_ensembl_seq.show(n=40, truncate=False)

    # create list of dataframes
    dfs = [df_uniprot_id, df_ref_seq, df_ensembl_seq]

    # create merged dataframe
    df = reduce(DataFrame.unionAll, dfs)
    # df_ensembl_seq.show(n=3000, truncate=False)


    df_uniprot = df.select([trim(col(c)).alias(c) for c in df.columns])
    df_uniprot.show(n=30, truncate=False)

    print("======= processing:" + uniprot_fasta_folder)
    uniprot_fasta = sql_context.read.text(uniprot_fasta_folder + "*.fasta.gz")
    uniprot_fasta = uniprot_fasta.filter(lower(uniprot_fasta.value).contains(">"))
    cols = ["Accession", "ProteinName", "Gene", "Organism", "TaxId", "EvidenceLevel"]
    uniprot_fasta = uniprot_fasta.rdd.map(uniprot_header_to_df).toDF().toDF(*cols)
    # uniprot_fasta.show(n=30)

    complete_uniprot = df_uniprot.join(uniprot_fasta, df_uniprot.AC == uniprot_fasta.Accession, "left").drop(uniprot_fasta.Accession)

    # This delete duplicated records by accession
    complete_uniprot = complete_uniprot.dropDuplicates(['AC', 'ID'])
    complete_uniprot = complete_uniprot.filter(complete_uniprot.ID.isNotNull())
    complete_uniprot.write.parquet(out_path, mode='append', compression='snappy')

    print_df = sql_context.read.parquet(out_path)
    print_df.show(n=3000, truncate=False)


if __name__ == "__main__":
    uniprot_mapping_to_parquet()
