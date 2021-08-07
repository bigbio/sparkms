import unittest

from pyteomics import mztab


class TestPSM2Peptable(unittest.TestCase):
  path_mztab1 = 'resources/PXD005942-Sample-25-out.mzTab'

  def test_mztab_to_dataframe(self):
    reader_mztab1 = mztab.MzTab(self.path_mztab1)

    # peptide_table = mztab_to_dataframe(reader_mztab1)
    assert (len(reader_mztab1.peptide_table)) == 2715
