#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
from datetime import datetime
from raptors.models.readers import Reader, ExcelReader, MongoReader
from raptors.models.writers import Writer, MongoWriter
from raptors.helpers.mongoutils import Mongo
from raptors.helpers.exceptions.modelsexceptions import CollectionDoesNotExistException
from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class Sync():
    """Bulk uploader that helps to migrate data from Excel
    to MongoDB
    """
    

    def __init__(self, org_db, org_tbl, des_db, des_tbl, 
            host=None, port=None, streaming=False, sensitive_colls=[]):
        """Initializer for Sync class"""
        self.reader          = Reader(ExcelReader(org_db, org_tbl))
        self.writer          = Writer(MongoWriter(des_db, des_tbl, host=host, port=port))
        self.streaming       = streaming
        self.sensitive_colls = sensitive_colls if sensitive_colls else [ 'ent_dump_from_finance', 'ent_dump_from_finance_old' ]
        self.is_sensitive    = des_tbl in self.sensitive_colls
        self.is_sfdc         = des_tbl == 'sfdc_raw_dump'
        self.clean_cols      = []
        try:
            if self.is_sensitive:
                self.clean_cols  = Reader(MongoReader('booking_dump_cols')).read_dict()
            if self.is_sfdc:
                self.clean_cols  = Reader(MongoReader('sfdc_dump_cols')).read_dict()
        except CollectionDoesNotExistException as e:
            print("[Error]: Couldn't proceed further due to \n\n{}".format(e.msg()))
        self.trash_query     = {}

    def execute(self):
        """Public method that executes the Syncing the data to MongoDB"""
        self._validate()
        if self.streaming:
            self._sync_by_streaming()
        else:
            self._sync_by_bulk()
        pass

    def _validate(self):
        """Checks the validity of existence of the file/table 
        and exiting data
        """
        self.reader.validate()
        self.writer.validate()
        return

    def _sync_by_streaming(self):
        """Synchronizes the template data into MongoDB by
        writing as the reading goes on
        """
        pass

    def _sync_by_bulk(self):
        """Synchronizes the template data in the order of 
        reading full data first and write them
        """
        self._read_and_cleanup()
        # if not self.is_sfdc:
        if self.is_sensitive:
            self._expunge_existing_data()
        else:
            self._expunge_all_existing_data()
        self.writer.write(self.reader.df)
        return

    def _read_and_cleanup(self):
        """Private method to read the data and clean up"""
        print("[Info]: Reading data...")
        self.reader.read() # Reads and stores the dataframe
        print("[Info]: Cleaning data...")
        self.reader.downcase_colnames() # Calling DataFrameHelper function through Reader class
        self.reader.fill_notapplicables() # By default, fill value is zero
        if self.is_sensitive or self.is_sfdc:
            self.reader.rename_columns(self.clean_cols)
            if self.is_sensitive:
                self.reader.delete_columns('not_to_be_mapped')
        if self.is_sensitive or self.is_sfdc:
            self.reader.add_timestamp() # Adding timestamp to all dataframe
        return

    def _expunge_all_existing_data(self):
        """Private method to clean up All existing data"""
        self._warn_user()
        self._expunge()
        return

    def _expunge_existing_data(self):
        """Private method to clean up the existing data"""
        field            = 'fiscal_week_id'
        weeks            = self.reader.get_uniques(field)
        self.trash_query = Mongo.make_OR(weeks, field)
        self._warn_user()
        self._expunge()
        return

    def _warn_user(self):
        """Warns the user with the hazardous deletion process"""
        recs_planned = self.writer.how_many_docs(self.trash_query)
        print("[Info]: {} row(s) existing! {} row(s) planned for trashing!?!".format(
            self.writer.recs_before(), recs_planned))
        return

    def _expunge(self):
        """Executes the removing of documents from the writable collection"""
        self.writer.trash_many(self.trash_query)
        return
        










