import unittest

from sparkms.commands.analysis.peptide_summary import get_protein_accession


class MyTestCase(unittest.TestCase):

  def test_protein_accessions(self):
    accessions = ["sp|P31271|HXA13_HUMAN", "tr|P31271|HXA13_HUMAN", "P31271", "HXA13_HUMAN", "gi|662189191|ref|XP_008488304.1|","ref|NP_004283.2"]
    results_accessions =["P31271", "P31271", "P31271", "HXA13_HUMAN", "XP_008488304.1", "NP_004283.2"]
    process_accessions = [get_protein_accession(value) for value in accessions]
    self.assertEqual(results_accessions, process_accessions)

if __name__ == '__main__':
  unittest.main()
