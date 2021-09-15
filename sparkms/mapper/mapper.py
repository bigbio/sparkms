"""
epimeter is a tool that determines how similar a given neo-epitope is to the
human proteome by calculating the distance between the peptide sequences.
epimeter has two functions: "index" and "query." The "index" function takes
a FASTA file of proteins as an input and indexes all of the protein k-mers
(user specifies k) into an Annoy Index. The "query" function then allows the
user to search for a specific neo-epitope candidate within the Annoy Index
allowing for a much more stream-lined and efficient search process than
current bioinformatics search tools such as BLAST.
"""
import argparse
from collections import defaultdict

import click

from sparkms.mapper.peptide_index import PeptideIndex

@click.group()
def cli1():
    pass

@cli1.command("index", help="index a fasta protein database")
@click.option("--fasta", help="Fasta database containing the protein sequences", required = True)
@click.option("--kmer", help="range of kmers for which annoy indexes need to be built", nargs=2, type=int, default=[8, 11], required=False)
@click.option("--index-name", help="Index file name", required=True)
def build_index(fasta, kmer, index_name):
  """builds protein from protein fasta and indexes kmers
  in protein into a Peptide Index, and saves the Peptide Index.
  Each set of kmers has its own annoy index in Peptide Index

  protein_fasta: path to file of protein seqs
  kmer_bounds: range of kmers for which annoy indexes need to be built
  index_name: name of dir where Peptide Index will be saved

  Return Value: PeptideIndex
  """
  print(kmer)
  min_k = int(kmer[0])
  max_k = int(kmer[1])
  peptide_index = PeptideIndex()
  protein = []
  with open(fasta) as protein_fasta:
    # skip the first header in file
    next(protein_fasta)
    item_numbers = defaultdict(int)
    for line in protein_fasta:
      if line.startswith('>'):
        # New protein! Process k-mers from last protein assembled
        # Start with the full protein
        protein = ''.join(protein)
        for k in range(min_k, max_k + 1):
          for i in range(len(protein) - k + 1):
            item_numbers[k] = item_numbers[k] + 1
            peptide_index.add_item(item_numbers[k], protein[i:i + k])
        protein = []
      # continue assembling the protein
      else:
        protein.append(line.strip())
    # update the index for the last protein in the file
    if len(protein) > 0:
      protein = ''.join(protein)
      for k in range(min_k, max_k + 1):
        for i in range(len(protein) - k + 1):
          item_numbers[k] = item_numbers[k] + 1
          peptide_index.add_item(item_numbers[k], protein[i:i + k])
  peptide_index.save(index_name)
  return peptide_index

@click.group()
def cli2():
    pass

@cli1.command("query", help="find peptides in database")
@click.option("--query-peptides", help="File with all the peptides to be map to the proteome", required = True)
@click.option("--index-name", help="Index file name", required=True)
@click.option("--output", help="Output tsv with peptides")
def query_epitope(query_peptides, index_name, output):
  """ takes each epitope from a file of epitopes, queries for the
  nearest neighbors, and then writes the nearest neighbors + distances
  for each epitope in one line of csv file

  epitopes: path to file of epitopes; assumes one epitope per line
  index_name: path to directory where PeptideIndex will be loaded from

  Return Value: none
  """
  nearest_neighbors = open(output, "w")
  peptide_index = PeptideIndex.PeptideIndex()
  peptide_index.load(index_name)
  with open(query_peptides) as epitopes:
    for line in epitopes:
      epitope = line.strip()
      # returns a tuple with the neighbors and the distances
      results = peptide_index.get_nns_by_epitope(epitope,
                                                 num_neighbors=8,
                                                 search_k=-1,
                                                 include_distances=True)
      # stores neighbors and distances from tuple as lists
      neighbors = results[0]
      distances = results[1]
      for i in range(len(neighbors)):
        # insert commas between the neighbors and respective distances
        # for the epitope
        nearest_neighbors.write(str(neighbors[i]) + "," + str(distances[i]))
        if i < len(neighbors) - 1:
          nearest_neighbors.write(",")
      nearest_neighbors.write("\n")
  nearest_neighbors.close()

cli = click.CommandCollection(sources=[cli1, cli2])

if __name__ == '__main__':
  cli()
