import unittest
from raptors.controllers.backup import Backup

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class BackupTest(unittest.TestCase):
    """Unit test to run test cases on Backup class
    """


    def setUp(self):
        """Initialization of BackupTest
        """
        self.home_dir = '~/Documents/Jeyaraj/My_Contents/Work/Commercial/Templates/XLSX'
        self.backup  = Backup({ 
            'coll'    : 'master_unique_names', 
            'db'      : 'test', 
            'all'     : False,
            'home_dir': self.home_dir,
            'out'     : None
        })
        self.backup2 = Backup({
            'coll'    : 'master_unique_names', 
            'db'      : None, 
            'all'     : False,
            'home_dir': self.home_dir,
            'out'     : None
        })
        self.backup3 = Backup({
            'coll'    : None, 
            'db'      : None, 
            'all'     : False,
            'home_dir': self.home_dir,
            'out'     : None
        })

    def test_option_all_is_false(self):
        """Tests whether all attribute is False
        """
        self.assertTrue(self.backup.all == False)
        return

    def test_option_coll_is_set_with_value(self):
        """Tests whether option 'coll' is having the value of 'master_unique_names'
        """
        self.assertTrue(self.backup.coll == 'master_unique_names')
        return

    def test_option_db_is_set_with_value(self):
        """Tests whether option 'db' is having the value of 'test'
        """
        self.assertTrue(self.backup.db == 'test')
        return

    def test_option_db_is_set_with_default_value(self):
        """Tests whether option 'db' is having the default value as 'ccsdm'
        """
        self.assertTrue(self.backup2.db == 'ccsdm')
        return

    def test_shell_cmd_delete_dump_dir_should_be(self):
        """Tests whether the shell_cmd_delete_dump_dir function 
        returns correct string
        """
        self.backup._set_attributes()
        matching_str = "rm -rf {}/dump".format(self.home_dir)
        self.assertTrue(self.backup.executable.shell_cmd_delete_dump_dir() == matching_str)
        return

    def test_shell_cmd_create_mongodump_should_be(self):
        """Tests whether the shell_cmd_mongodump function returns correct string
        """
        self.backup._set_attributes()
        matching_str = "mongodump --out {} --db test --collection master_unique_names".format(
                self.backup.executable.dump_directory)
        self.assertTrue(self.backup.executable.shell_cmd_mongodump() == matching_str)
        return

    def test_shell_cmd_move_tar_to_location_should_be(self):
        """Tests whether the shell_cmd_move_tar_to_location function returns correct string
        """
        self.backup._set_attributes()
        matching_str = "mv {} {}".format(
                self.backup.executable.dumpname, self.backup.executable.base_dir)
        self.assertTrue(self.backup.executable.shell_cmd_move_tar_to_location() == matching_str)
        return

    def test_without_db_and_coll_names_shell_cmd_create_mongodump_should_be(self):
        """Tests whether the shell_cmd_mongodump without db and coll names returns correct string 
        """
        self.backup3._set_attributes()
        matching_str = "mongodump --out {} --db ccsdm".format(
                self.backup3.executable.dump_directory)
        self.assertTrue(self.backup3.executable.shell_cmd_mongodump() == matching_str)
        return

