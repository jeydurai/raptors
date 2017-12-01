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


class ExcelFileDoesNotExistException(Exception):
    """Excel file existence Error
    """


    def __init__(self, path):
        """Initializer for ExcelFileDoesNotExistException class"""
        super().__init__("Either the file does not exist or the path is not a file")
        self.path = path

    def msg(self):
        """Special Error Message"""
        return "{} is not a file or does not exist!!!".format(self.path)
        

class ExcelSheetDoesNotExistException(Exception):
    """Excel Sheet existence Error
    """


    def __init__(self, sheet):
        """Initializer for ExcelSheetDoesNotExistException class"""
        super().__init__("Given Sheet does not exist")
        self.sheet = sheet

    def msg(self):
        """Special Error Message"""
        return "{} does not exist!!!".format(self.sheet)
        

class CollectionDoesNotExistException(Exception):
    """MongoDB Collection does not exist Error
    """


    def __init__(self, coll):
        """Initializer for CollectionDoesNotExistException class"""
        super().__init__("Given Collection does not exist")
        self.coll = coll

    def msg(self):
        """Special Error Message"""
        return "{} does not exist!!!".format(self.coll)
        

class MappingRowsExceededException(Exception):
    """DataFrame mapping (merging) rows exceeded
    """


    def __init__(self, rows_bef, rows_aft):
        """Initializer for MappingRowsExceededException class"""
        super().__init__("Rows after mapping is greater than that before mapping")
        self.rows_bef = rows_bef
        self.rows_aft = rows_aft

    def msg(self):
        """Special Error Message"""
        return "Expected {} row(s), but found {} row(s)".format(self.rows_bef, self.rows_aft)
        
        



















