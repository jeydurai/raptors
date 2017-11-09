#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
from datetime import datetime

from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"

class Backup():
    """Holds the data backing up functionalities
    """


    def __init__(self, opts):
        """Initializer of Backup class"""
        self.all        = opts['all']
        self.out        = opts['out']
        self.db         = opts['db'] if opts['db'] is not None else 'ccsdm'
        self.coll       = opts['coll']
        self.home_dir   = opts['home_dir']
        self.executable = Executable(self.home_dir)

    def execute(self):
        """Public method to execute the backup process"""
        self._set_attributes()
        self._run_shell_command()
        return

    def _set_attributes(self):
        """Private method to set all attributes required
        for execution
        """
        self.executable.dump_directory = self.home_dir if self.out is None else self.out
        self.executable.db             = self.db
        self.executable.coll           = self.coll
        return

    def _run_shell_command(self):
        """Private method to execute the shell command"""
        print("[Info]: Executing Shell Commands...\n")
        print("[Info]: Deleting existing dump directory...\n")
        os.system(self.executable.shell_cmd_delete_dump_dir())
        print("[Info]: Creating MongoDB dump...\n")
        os.system(self.executable.shell_cmd_mongodump())
        print("[Info]: Creating tar file...\n")
        os.system(self.executable.shell_cmd_create_tar())
        print("[Info]: Moving the tar file to the correct location...\n")
        os.system(self.executable.shell_cmd_move_tar_to_location())
        print("[Info]: Deleting newly created dump directory...\n")
        os.system(self.executable.shell_cmd_delete_dump_dir())
        return
        
        

class Executable():
    """Data Container to store executable strings
    """
   

    def __init__(self, base_dir, create_tar=True):
        """Initializer for Executable"""
        self.base_dir        = base_dir
        self.create_tar      = create_tar
        self.dumpname        = "dump_{}{}".format(datetime.now().strftime('%d-%m-%Y-%H%M'), ".tar.gz")
        self._db             = ''
        self._coll           = ''
        self._dump_directory = ''
        self._mongo_dump     = ''

    @property
    def dump_directory(self):
        """Getter to return dump direcotry"""
        return self._dump_directory

    @dump_directory.setter
    def dump_directory(self, value):
        """Setter to store dump direcotry"""
        self._dump_directory = os.path.join(value, 'dump')
        return

    def shell_cmd_delete_dump_dir(self):
        """Makes a shell command string for deleting dump directory"""
        return "rm -rf {}".format(self.dump_directory)

    @property
    def db(self):
        """Getter for _db property"""
        return self._db

    @db.setter
    def db(self, value):
        """Setter for _db property"""
        self._db = value
        
    @property
    def coll(self):
        """Getter for _coll property"""
        return self._coll

    @coll.setter
    def coll(self, value):
        """Setter for _coll property"""
        self._coll = value

    def shell_cmd_mongodump(self):
        """Makes a shell command string for 'mongodump'"""
        cmd = 'mongodump '
        cmd += "--out {}".format(self.dump_directory)
        cmd += " --db {}".format(self.db) if self.db else ""
        cmd += " --collection {}".format(self.coll) if self.coll else ""
        return cmd

    def shell_cmd_create_tar(self):
        """Makes a shell command string for creating tar file from dump file"""
        if not self.create_tar:
            return
        return "tar -zcvf {} {}".format(self.dumpname, self.dump_directory)
        
    def shell_cmd_move_tar_to_location(self):
        """Makes a shell command string for moving tar file to right location"""
        if not self.create_tar:
            return
        return "mv {} {}".format(self.dumpname, self.base_dir)
        
        





        
