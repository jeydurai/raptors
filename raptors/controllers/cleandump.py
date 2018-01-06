#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
from datetime import datetime
from raptors.models.readers import EntBookingDumpReader, TechSpec1Reader, SFDCDumpReader
from raptors.models.readers import MasterUniqueNamesReader, MongoReader, SL5ToSegmentsReader
from raptors.models.writers import Writer, MongoWriter
from raptors.helpers.mongoutils import Mongo
from raptors.helpers.exceptions.modelsexceptions import MappingRowsExceededException
from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class CleanBookingDump():
    """Cleans/Validates the ent_dump_from_finance data and 
    creates a new collection 'booking_dump'
    """

    edff_collname        = 'ent_dump_from_finance' # edff stands for 'ent_dump_from_finance'
    techmapper_collname  = 'tech_spec1'
    segmapper_collname   = 'sl5_to_segments'
    uniquenames_collname = 'master_unique_names'
   

    def __init__(self, history, years, comm, des_tbl, des_db, host=None, port=None):
        """Initializer of CleanBookingDump
        """
        self.history     = history
        self.years       = years if years else self._get_years(history, '2018')
        self.comm        = comm
        self.sl3         = self._get_sales_level_3()
        self.finmonths   = self._make_fin_months()
        self.reader      = EntBookingDumpReader(MongoReader(self.edff_collname))
        self.techmapper  = TechSpec1Reader(MongoReader(self.techmapper_collname))
        self.segmapper   = SL5ToSegmentsReader(MongoReader(self.segmapper_collname))
        self.uniquenames = MasterUniqueNamesReader(MongoReader(self.uniquenames_collname))
        self.writer      = Writer(MongoWriter(des_db, des_tbl, host=host, port=port))
        self.trash_query = {}

    def _get_years(self, history, curr_year):
        """Private method to make years array based on history/years options
        """
        years       = []
        num_of_yrs  = history if history > 0 else 2
        for h in range(num_of_yrs):
            years.append(str(int(curr_year)-h))
        return years

    def execute(self):
        """Public method to execute the whole process"""
        self._read_dump()
        self._cleanup_data()
        self._read_mappers()
        # self._remove_timestamps()
        self._execute_mapping()
        self._write_dump()
        return

    def _write_dump(self):
        """Writes Cleaned Dump into the new collection"""
        self._expunge_all_existing_data()
        self.writer.write(self.reader.df)
        return

    def _remove_timestamps(self):
        """Removes the timestamps from the dataframes"""
        cols_to_be_deleted = 'timestamp'
        print("Dump's columns: {}".format(self.reader.df.columns))
        self.reader.delete_columns(cols_to_be_deleted)
        print("Dump's columns: {}".format(self.techmapper.df.columns))
        self.techmapper.delete_columns(cols_to_be_deleted)
        print("Dump's columns: {}".format(self.setmapper.df.columns))
        self.segmapper.delete_columns(cols_to_be_deleted)
        print("Dump's columns: {}".format(self.uniquenames.df.columns))
        self.uniquenames.delete_columns(cols_to_be_deleted)
        return
        
    def _cleanup_data(self):
        """Private method to clean up data structure and 
        map other information
        """
        self.reader.rename_colnames_neatly()
        self.reader.upcase_customernames()
        self.reader.upcase_partnernames()
        self.reader.make_fiscalyearid_column()
        self.reader.make_fiscalmonthid_column()
        self.reader.make_prodserv_column()
        self.reader.make_cloudflag_column()
        self.reader.make_tiercode_column()
        self.reader.validate_sl4()
        return

    def _read_dump(self):
        """Private method to read and store the dump data"""
        print("[Info]: Reading 'ent_dump_from_finance' data")
        qry = { 'sales_level_3': self.sl3 } if self.sl3 else {}
        loop_params = { 'field': 'fiscal_period_id', 'params': self.finmonths }
        self.reader.read(qry=qry, loop_params=loop_params)
        return

    def _read_mappers(self):
        """Private method to read and store the Mapping data"""
        print("[Info]: Reading 'tech_spec1' data")
        self.techmapper.read()
        print("[Info]: Reading 'sl5_to_segments' data")
        self.segmapper.read()
        print("[Info]: Reading 'master_unique_names' data")
        self.uniquenames.read()
        return

    def _execute_mapping(self):
        """Executes All Mapping"""
        self.techmapper.remove_dedundancy()
        self.uniquenames.upcase_names()
        self.uniquenames.upcase_accnames()
        self.uniquenames.upcase_grpnames()
        self.uniquenames.remove_dedundancy()
        self.uniquenames.transform_names_to_customername()
        try:
            self.reader.map_technologies(self.techmapper)
            self.reader.map_segments(self.segmapper)
            self.reader.map_uniquenames(self.uniquenames)
        except MappingRowsExceededException as e:
            print("[Error]: Couldn't proceed further due to \n\n{}".format(e.msg()))

        self.reader.validate_mapping()
        return

    def _get_sales_level_3(self):
        """Private method to return the correct Sales Level 3"""
        if self.comm:
            return 'INDIA_COMM_1'
        return None

    def _make_fin_months(self):
        """Private method to make financial months using years
        """
        months = []
        for i in range(1, 13):
            months.append(str(i).zfill(2))
        return [ int(year + month) for year in self.years for month in months]

    def _expunge_all_existing_data(self):
        """Private method to clean up All existing data"""
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


class CleanSFDCDump():
    """Cleans/Validates the 'sfdc_raw_dump' data and 
    creates a new collection 'sfdc_dump'
    """

    raw_collname         = 'sfdc_raw_dump'
    techmapper_collname  = 'tech_spec1'
    segmapper_collname  = 'sl5_to_segments'
   

    def __init__(self, comm, des_tbl, des_db, host=None, port=None):
        """Initializer of CleanSFDCDump
        """
        self.comm        = comm
        self.sl3         = self._get_sales_level_3()
        self.reader      = SFDCDumpReader(MongoReader(self.raw_collname))
        self.techmapper  = TechSpec1Reader(MongoReader(self.techmapper_collname))
        self.segmapper   = SL5ToSegmentsReader(MongoReader(self.segmapper_collname))
        self.writer      = Writer(MongoWriter(des_db, des_tbl, host=host, port=port))
        self.trash_query = {}

    def execute(self):
        """Public method to execute the whole process"""
        self._read_dump()
        self._cleanup_data()
        self._read_mappers()
        self._execute_mapping()
        self._write_dump()
        return

    def _write_dump(self):
        """Writes Cleaned Dump into the new collection"""
        self._expunge_all_existing_data()
        self.writer.write(self.reader.df)
        return
        
    def _cleanup_data(self):
        """Private method to clean up data structure and 
        map other information
        """
        self.reader.upcase_customernames()
        self.reader.make_fiscalyearid_column()
        self.reader.make_fiscalquarter_column()
        self.reader.make_fiscalquarterid_column()
        self.reader.make_fiscalmonthid_column()
        self.reader.make_prodserv_column()
        self.reader.make_pastdue_column()
        return

    def _read_dump(self):
        """Private method to read and store the dump data"""
        print("[Info]: Reading 'sfdc_raw_dump`' data")
        qry = { 'sales_level_3': self.sl3 } if self.sl3 else {}
        self.reader.read(qry=qry)
        return

    def _read_mappers(self):
        """Private method to read and store the Mapping data"""
        print("[Info]: Reading 'tech_spec1' data")
        self.techmapper.read()
        print("[Info]: Reading 'sl5_to_segments' data")
        self.segmapper.read()
        return

    def _execute_mapping(self):
        """Executes All Mapping"""
        self.techmapper.remove_dedundancy()
        try:
            self.reader.map_technologies(self.techmapper)
            self.reader.map_segments(self.segmapper)
        except MappingRowsExceededException as e:
            print("[Error]: Couldn't proceed further due to \n\n{}".format(e.msg()))

        self.reader.validate_mapping()
        return

    def _get_sales_level_3(self):
        """Private method to return the correct Sales Level 3"""
        if self.comm:
            return 'INDIA_COMM_1'
        return None

    def _expunge_all_existing_data(self):
        """Private method to clean up All existing data"""
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





        
