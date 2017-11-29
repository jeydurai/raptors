import unittest
from raptors.controllers.sync import Sync

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class SyncTest(unittest.TestCase):
    """Unit test to run test cases on Sync class
    """


    def setUp(self):
        """Initialization of SyncTest
        """
        self.homedir = '~/Documents/Jeyaraj/My_Contents/Work/Commercial/Templates/XLSX'
        filename     = 'FinBI_Booking_Dump_Downloaded_Q2_on_2017-11-13_at_0935hrs.xlsx'
        filepath     = '~/Documents/Jeyaraj/My_Contents/Work/Commercial/Dumps/Booking_Dump'
        org_db       = "{}/{}".format(filepath, filename)
        org_tbl      = 'Commercial_Booking'
        des_db       = 'ccsdm'
        des_tbl      = 'ent_dump_from_finance'
        des_tbl2     = 'master_unique_names'
        des_tbl3     = 'ent_dump_from_finance_old'
        self.sync    = Sync(org_db, org_tbl, des_db, des_tbl)
        self.sync2   = Sync(org_db, org_tbl, des_db, des_tbl2)
        self.sync3   = Sync(org_db, org_tbl, des_db, des_tbl3)

    def test_option_is_sensitive_true(self):
        """Test whether 'is_sensitive' variable is True"""
        self.assertTrue(self.sync.is_sensitive)
        self.assertTrue(self.sync3.is_sensitive)
        return
        
    def test_clean_cols_not_empty(self):
        """Test Whether clean_colls instance variable is not empty"""
        self.assertTrue(len(self.sync.clean_cols) > 0)
        self.assertTrue(len(self.sync3.clean_cols) > 0)
        return
        
    def test_option_is_sensitive_false(self):
        """Test whether 'is_sensitive' variable is False"""
        self.assertEqual(False, self.sync2.is_sensitive)
        return
        
    def test_clean_cols_is_empty(self):
        """Test Whether clean_cols instance variable is empty"""
        self.assertTrue(len(self.sync2.clean_cols) == 0)
        return
        
    def test_trash_query_empty(self):
        """Test Whether 'trash_query' variable is empty after initialization"""
        self.assertTrue(len(self.sync.trash_query.keys()) == 0)
        self.assertTrue(len(self.sync2.trash_query.keys()) == 0)
        self.assertTrue(len(self.sync3.trash_query.keys()) == 0)
        return
