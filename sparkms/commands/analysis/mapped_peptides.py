import click
import pandas as pd


@click.command('summary-mapped-peptides', short_help='')
@click.option('--peptide-mapped-file', help="Input peptide summary mapped file. ie., /path/to/", required=True)
@click.option('--out-peptide-file', help="Output path to store parquets. ie., /out/path", required=True)
def summary_mapped_peptides(peptide_mapped_file, out_peptide_file):
  """

  This function takes the ouput of proteomapper (http://www.tppms.org/tools/pm/) and get a file of only the peptides
  that maps unique to one protein in uniprot.

  :param peptide_mapped_file: output of proteomapper peptides.
  :param out_peptide_file: Output only the peptides that map to only one protein.
  :return:
  """

  df_pep_original = pd.read_csv(peptide_mapped_file, sep='\t')
  df_pep_original = df_pep_original.groupby("peptide").agg({'protein':lambda x: list(x)})
  df_pep_original['protein_count'] = df_pep_original['protein'].str.len()
  df_pep_original = df_pep_original[df_pep_original['protein_count'] == 1]
  df_pep_original['protein_count'] = df_pep_original.protein.apply(lambda x: len(x[0].split()))
  df_pep_original = df_pep_original[df_pep_original['protein_count'] == 1]
  print(df_pep_original.head(10))

  df_pep_original.to_csv(out_peptide_file, sep='\t', header=True)


if __name__ == '__main__':
    summary_mapped_peptides()
