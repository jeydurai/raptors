#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
from datetime import datetime
from raptors.helpers.exceptions.modelsexceptions import MappingRowsExceededException
import pandas as pd
import string

from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class DataFrameHelper():
    """Holds and abstracts all the functionalities of 
    pandas (and Series) tool
    """


    def __init__(self):
        """Initializer of DataFrameHelper"""
        self.df   = None
        self.rows = 0
        self.cols = 0

    def _upcase(self, x):
        """Private method to checks and changes the case into upper"""
        if isinstance(x, int):
            return x
        return x.upper()

    def _downcase(self, x):
        """Private method to checks and changes the case into lower"""
        if isinstance(x, int):
            return x
        return x.lower()

    def rename_columns(self, cols_mapper):
        """Public method to rename the column names"""
        self.df.rename(columns=cols_mapper, inplace=True)
        return

    def make_new_column(self, mapper, colname, new_colname=None):
        """Makes a new column based on the mapper provided"""
        if new_colname is None:
            self.df.loc[:, colname] = self.df.loc[:, colname].map(mapper)
        else:
            self.df.loc[:, new_colname] = self.df.loc[:, colname].map(mapper)
        return

    def remove_duplicates(self, cols, map_desc='Redundancy removal'):
        """Removes the duplicate entries in the dataframe"""
        print("Before performing {} mapping, the dataframe contained {} row(s) {} col(s)".format(map_desc, self.rows, self.cols))
        self.df.drop_duplicates(cols, inplace=True)
        self.rows, self.cols = self.df.shape
        print("After performing {} mapping, the dataframe contains {} row(s) {} col(s)".format(map_desc, self.rows, self.cols))
        return

    def left_join(self, other, on, map_desc=''):
        """Left Join two dataframes"""
        rows_bef = self.rows
        print("Before performing {} mapping, the dataframe contained {} row(s) {} col(s)".format(map_desc, self.rows, self.cols))
        self.df = pd.merge(self.df, other, on=on, how='left')
        self.rows, self.cols = self.df.shape
        rows_aft = self.rows
        print("After performing {} mapping, the dataframe contains {} row(s) {} col(s)".format(map_desc, self.rows, self.cols))
        if rows_aft != rows_bef:
            raise MappingRowsExceededException(rows_bef, rows_aft)
        return

    def delete_columns(self, cols):
        """Public method to drop columns"""
        self.df.drop(cols, axis=1, inplace=True)
        return
    
    def upcase_column(self, colname):
        """Public method to change a column's case into upper"""
        self.make_new_column(lambda x: self._upcase(x), colname)
        return
        
    def downcase_column(self, colname):
        """Public method to change a column's case into lower"""
        self.make_new_column(lambda x: self._downcase(x), colname)
        return
        
    def upcase_colnames(self):
        """Public method to change the column names case to upper"""
        self.df.rename(columns=str.upper, inplace=True)
        return

    def downcase_colnames(self):
        """Public method to change the column names case to lower"""
        self.df.rename(columns=str.lower, inplace=True)
        return

    def fill_notapplicables(self, val=0):
        """Public method to fill the not applicables with suitable value"""
        self.df.fillna(value=val, inplace=True)
        return

    def get_uniques(self, field):
        """Public method to extract unique field data as a list"""
        return self.df[field].unique().tolist()

    def fill_column_by_mask(self, masks, colname):
        """Fills the columns with a text/string/content by mask"""
        for filler, mask in masks.items():
            self.df.loc[mask, colname] = filler
        return
        

        

        
       






