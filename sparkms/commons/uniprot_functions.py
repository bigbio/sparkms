
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


def get_protein_name_from_header(uniprot_protein_header):
  """
  This function parse the protein name from Uniprot protein fasta header

  :param uniprot_protein_header:
  :return:
  """
  protein_name_accession = uniprot_protein_header.split("OS")[0]
  name_member = protein_name_accession.split(" ")
  name_member = name_member[1:(len(name_member) - 1)]

  return " ".join(name_member).strip()
