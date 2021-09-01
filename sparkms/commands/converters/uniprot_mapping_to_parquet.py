import click
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, length
from pyspark.sql.functions import col
from pyspark.sql.functions import lower

def get_info_from_header(uniprot_header, prefix):
  """
  This function parse from a Uniprot header the corresponding properties Genes, Organism, etc. The
  structure of the header is the following:
  >sp|P0C9T5|5054R_ASFWA Protein MGF 505-4R OS=African swine fever virus (isolate Warthog/Namibia/Wart80/1980) OX=561444 GN=War-038 PE=3 SV=1'

  The prefix is used by Uniprot to name properties such as Gene name GN or Organism OS.

  :param uniprot_header: Uniprot fasta header
  :param prefix: Uniprot property prefix (name)
  :return: Value for property.
  """

  if (prefix + "=") not in uniprot_header:
    return None
  # finds the prefix
  index = uniprot_header.index(prefix + "=")
  # remove it
  name = uniprot_header[index:][3:]
  # figure out if there is anothe = sign to split the string
  # otherwise, the prefix we looked for is the last one anyway
  if "=" in name:
    name = name.split("=")[0]
    # here each = sign in FASTA is preceded by 2 characters that we must remove
    name = name[0:-2]
    name = name.strip()
  else:
    name = name.strip()
  return name


def get_PE(uniprot_header):
  """
  This function get the Peptide Evidence level for uniprot protein
  :param uniprot_header: Uniprot protein fasta header
  :return: Peptide Evidence level
  """
  pe = get_info_from_header(uniprot_header, "PE")
  if pe is not None:
    return int(pe)


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

  gene_name = get_info_from_header(uniprot_header, "GN")
  result.append(gene_name)
  organism  = get_info_from_header(uniprot_header, "OS")
  result.append(organism)
  pe        = get_PE(uniprot_header)
  result.append(pe)

  return result


@click.command('uniprot-mapping-to-parquet', short_help='Command to convert id mapping file to parquet')
@click.option('-i', '--input-id-mapping', help="Input uniprot mapping tab file  ie., idmapping_selected.tab.2015_03.gz", required=True)
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
  df = df.select(df.columns[:2]).toDF("AC", "ID")
  df_uniprot = df.select([trim(col(c)).alias(c) for c in df.columns])
  df_uniprot.show(n = 30)

  print("======= processing:" + uniprot_fasta_folder)
  uniprot_fasta = sql_context.read.text(uniprot_fasta_folder + "*.fasta.gz")
  uniprot_fasta = uniprot_fasta.filter(lower(uniprot_fasta.value).contains(">"))
  cols = ["Accession", "Gene", "Organism", "EvidenceLevel"]
  uniprot_fasta = uniprot_fasta.rdd.map(uniprot_header_to_df).toDF().toDF(*cols)
  uniprot_fasta.show(n=30)

  complete_uniprot = df_uniprot.join(uniprot_fasta,df_uniprot.AC ==  uniprot_fasta.Accession,"left").drop(uniprot_fasta.Accession)
  complete_uniprot.filter(complete_uniprot.Gene.isNotNull()).show(n=30)
  df_uniprot.write.parquet(out_path, mode='append', compression='snappy')

if __name__ == "__main__":
  uniprot_mapping_to_parquet()
