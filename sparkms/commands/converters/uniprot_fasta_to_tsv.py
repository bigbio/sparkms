from Bio import SeqIO
import click
import csv

from sparkms.commons.uniprot_functions import get_protein_name_from_header, get_info_from_header, get_PE


@click.command('fasta-to-tsv', short_help='')
@click.option('--uniprot-fasta', help='Uniprot fasta file folder', required=True)
@click.option('--uniprot-tsv', help="Uniprot in tsv format", required=True)
def fasta_to_tsv(uniprot_fasta, uniprot_tsv):

  record_iterator = SeqIO.parse(uniprot_fasta,"fasta")
  with open(uniprot_tsv, 'w', newline='') as f_output:
    tsv_output = csv.writer(f_output, delimiter='\t')

    tsv_output.writerow(["Accession", "ProteinName", "Gene", "Organism", "TaxId", "EvidenceLevel", "Sequence"])
    for record in record_iterator:
      result = list()
      uniprot_header = record.description
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

      result.append(str(record.seq))
      tsv_output.writerow(result)



if __name__ == '__main__':
    fasta_to_tsv()
