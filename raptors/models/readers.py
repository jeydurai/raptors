#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
from datetime import datetime
from pymongo import MongoClient
import pandas as pd
import string
from raptors.helpers.exceptions.modelsexceptions import ExcelFileDoesNotExistException
from raptors.helpers.exceptions.modelsexceptions import ExcelSheetDoesNotExistException
from raptors.helpers.exceptions.modelsexceptions import CollectionDoesNotExistException
from raptors.helpers.pandashelpers import DataFrameHelper

from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class Reader(DataFrameHelper):
    """Traits contains the common interface of 
    readable functionalities
    """


    def __init__(self, reader):
        """Initializer for Readable class"""
        self.reader = reader
        self.df     = None

    def read(self, qry={}, loop_params=None):
        """Hook method to read the data irrespective of the engine 
        as abstracted interface
        """
        if loop_params is None:
            self.df = self.reader.read(qry)
        else:
            params = loop_params['params']
            field  = loop_params['field']
            self.df = pd.DataFrame()
            for param in params:
                qry[field] = param
                dframe     = self.reader.read(qry)
                if not dframe.empty:
                    self.df = self.df.append(dframe)
        self.rows, self.cols = self.df.shape
        print("{} row(s) {} col(s) have been read.".format(self.rows, self.cols))
        return

    def read_dict(self, qry={}):
        """Hook method to read the data in dict format 
        irrespective of the engine as abstracted interface
        """
        return self.reader.read_dict(qry)

    def trash_one(self, qry):
        """Hook method to remove just one document/row 
        irrespective of the engine as abstracted interface
        """
        return self.reader.trash_one(qry)

    def trash_many(self, qry):
        """Hook method to remove many documents/rows
        irrespective of the engine as abstracted interface
        """
        return self.reader.trash_many(qry)

    def validate(self):
        """Hook method to validate the specific reader
        """
        self.reader.validate()
        return
    

class AggregationReader(Reader):
    """Traits contains the common interface of 
    aggregatable functionalities
    """


    def __init__(self, reader):
        """Initializier for AggregationReader class"""
        super().__init__(reader)
        self.df = pd.DataFrame()

    def read(self, agg_pipes=None):
        """Overridden hook method to read data using MongoDB aggregation"""
        for pipe in agg_pipes:
            dframe = self.reader.agg(pipe)
            if not dframe.empty:
                self.df = self.df.append(dframe)
        self.rows, self.cols = self.df.shape
        print("{} row(s) {} col(s) have been read.".format(self.rows, self.cols))
        return


class SalesDumpReader(Reader):
    """Adaptor contains the common interface of editable 
    functionalities of 'ent_dump_from_finance' & 'sfdc_raw_dump' collections
    """


    def __init__(self, reader):
        """Initializer for Readable class"""
        super().__init__(reader)

    def upcase_customernames(self):
        """Public hook method to change 'customer_name' case into upper"""
        self.upcase_column('customer_name')
        return
        
    def downcase_customernames(self):
        """Public hook method to change 'customer_name' case into lower"""
        self.downcase_column('customer_name')
        return
        
    def upcase_partnernames(self):
        """Public hook method to change 'partner_name' case into upper"""
        self.upcase_column('partner_name')
        return
        
    def downcase_partnernames(self):
        """Public hook method to change 'partner_name' case into lower"""
        self.downcase_column('partner_name')
        return
        
    def map_technologies(self, techmapper):
        """Maps the technology/architecture data into 'ent_dump_from_finance' data"""
        self.left_join(techmapper.df, on=techmapper.mappable_cols, map_desc='Technology Mapping')
        return

    def map_segments(self, segmapper):
        """Maps the OD, RM, Segments data into 'ent_dump_from_finance' & 'sfdc_raw_dump' data"""
        self.left_join(segmapper.df, on=segmapper.mappable_cols, map_desc='Segment Mapping')
        return

    def map_uniquenames(self, uniquenames):
        """Maps the Unique names, vertical into 'ent_dump_from_finance' data"""
        self.left_join(uniquenames.df, on=uniquenames.mappable_cols, map_desc='Uniquenames Mapping')
        return

    def validate_sl4(self):
        """Public method to correct sales_level_4"""
        print("[Info]: Validating SL4 column (Reassigning into one unique)...")
        base_colname = 'sales_level_4'
        masks = { 
            'INDIA_COMM_SW_GEO' : ((self.df.loc[:, base_colname] == 'INDIA_COMM_WST') | (self.df.loc[:, base_colname] == 'INDIA_COMM_STH')),
            'INDIA_COMM_NE_GEO' : (self.df.loc[:, base_colname] == 'INDIA_COMM_NORTH_EAST'),
            'INDIA_COMM_MISC'   : (self.df.loc[:, base_colname] == 'INDIA_COMM_1-MISCL4')
        }
        self.fill_column_by_mask(masks, base_colname)
        return


class EntBookingDumpReader(SalesDumpReader):
    """Traits contains the common interface of 
    readable functionalities of 'ent_dump_from_finance' collection
    """


    def __init__(self, reader):
        """Initializer for Readable class"""
        super().__init__(reader)
        self.cols_renamable = {
            'tms_sales_allocated_bookings_base_list' : 'base_list',
            'tbm' : 'sales_agent'
        }

    def rename_colnames_neatly(self):
        """Hook method to rename the column names into readable"""
        self.rename_columns(self.cols_renamable)
        return

    def make_fiscalyearid_column(self):
        """Public method to make fiscal_year_id from fiscal_quarter_id"""
        self.make_new_column(lambda x: x[:4], 'fiscal_quarter_id', new_colname='fiscal_year_id')
        return

    def make_fiscalquarter_column(self):
        """Public method to make fiscal_quarter from fiscal_quarter_id"""
        self.make_new_column(lambda x: str(x)[-2:], 'fiscal_quarter_id', new_colname='fiscal_quarter')
        return
        
    def make_fiscalmonthid_column(self):
        """Public method to make fiscal_month_id from fiscal_period_id"""
        self.make_new_column(lambda x: str(x)[-2:], 'fiscal_period_id', new_colname='fiscal_month_id')
        return
        
    def make_prodserv_column(self):
        """Public method to create 'prod/serv' column"""
        print("[Info]: Making 'prod_serv' column...")
        self.make_new_column({ 'N': 'products', 'Y': 'services' }, 'services_indicator', new_colname='prod_serv')
        return
        
    def make_cloudflag_column(self):
        """Public method to create 'cloud_flag' column"""
        print("[Info]: Making 'cloud_flag' column...")
        base_colname = 'bookings_adjustments_code'
        masks = { 
            'N': self.df.loc[:, base_colname].str.startswith('L'),
            'Y': (-self.df.loc[:, base_colname].str.startswith('L'))
        }
        self.fill_column_by_mask(masks, 'cloud_flag')
        return
        
    def make_tiercode_column(self):
        """Public method to create 'tier_code' column"""
        print("[Info]: Making 'tier_code' column...")
        base_colname = 'bookings_adjustments_type'
        masks = { 
            'POS'       : ((self.df.loc[:, base_colname].str.startswith('POS')) | (self.df.loc[:, base_colname].str.startswith('DSV'))),
            'New Paper' : (-((self.df.loc[:, base_colname].str.startswith('POS')) | (self.df.loc[:, base_colname].str.startswith('DSV'))))
        }
        self.fill_column_by_mask(masks, 'tier_code')
        return
        
    def validate_mapping(self):
        """Validates whether any unmapped data available"""
        self._validate_mapping_technologies()
        self._validate_mapping_segements()
        self._validate_mapping_uniquenames()
        return

    def _validate_mapping_technologies(self):
        """Validates whether any unmapped data available in technology mapping"""
        mask = pd.isnull(self.df.loc[:, 'arch2'])
        unmapped = self.df.loc[mask, :].shape[0]
        if unmapped > 0:
            print("{} row(s) of unmapped 'internal_sub_business_entity_name' data found".format(unmapped))
            self.df.loc[mask, ['internal_sub_business_entity_name', 'arch2']].to_excel('unmapped_technologies.xlsx', index=False)
        return
        
    def _validate_mapping_segements(self):
        """Validates whether any unmapped data available in segments mapping"""
        mask = pd.isnull(self.df.loc[:, 'rm_name'])
        unmapped = self.df.loc[mask, :].shape[0]
        if unmapped > 0:
            print("{} row(s) of unmapped 'sales_level_5' data found".format(unmapped))
            self.df.loc[mask, ['sales_level_5', 'rm_name']].to_excel('unmapped_segments.xlsx', index=False)
        return
        
    def _validate_mapping_uniquenames(self):
        """Validates whether any unmapped data available in uniquenames mapping"""
        mask = pd.isnull(self.df.loc[:, 'acc_name'])
        unmapped = self.df.loc[mask, :].shape[0]
        if unmapped > 0:
            print("{} row(s) of unmapped unique customers data found".format(unmapped))
            self.df.loc[mask, ['customer_name', 'grp_name']].to_excel('unmapped_unique_customers.xlsx', index=False)
        return
        

class SFDCDumpReader(SalesDumpReader):
    """Traits contains the common interface of 
    readable functionalities of 'sfdc_raw_dump' collection
    """


    def __init__(self, reader):
        """Initializer for Readable class"""
        super().__init__(reader)
        self.cols_renamable = {
            'tms_sales_allocated_bookings_base_list' : 'base_list',
            'tbm' : 'sales_agent'
        }

    def make_pastdue_column(self):
        """Public method to validate 'past_due' column"""
        print("[Info]: Validating 'past_due' column...")
        base_colname = 'no_of_days_past_ebd'
        masks = { 
            'FALSE' : (self.df.loc[:, base_colname] < 0),
            'TRUE'  : (self.df.loc[:, base_colname] >= 0),
        }
        self.fill_column_by_mask(masks, 'past_due')
        return

    def make_fiscalyearid_column(self):
        """Public method to make fiscal_year_id from fiscal_period"""
        self.make_new_column(lambda x: str(x)[-4:], 'fiscal_period', new_colname='fiscal_year_id')
        return

    def make_fiscalquarter_column(self):
        """Public method to make fiscal_quarter from fiscal_period"""
        self.make_new_column(lambda x: str(x)[:2], 'fiscal_period', new_colname='fiscal_quarter')
        return
        
    def make_fiscalquarterid_column(self):
        """Public method to make fiscal_quarter_id from fiscal_period"""
        self.make_new_column(lambda x: str(x)[-4:] + str(x)[:2], 'fiscal_period', new_colname='fiscal_quarter_id')
        return
        
    def make_fiscalmonthid_column(self):
        """Public method to make fiscal_month_id from fiscal_month"""
        self.make_new_column(lambda x: x, 'fiscal_month', new_colname='fiscal_month_id')
        return
        
    def make_prodserv_column(self):
        """Public method to create 'prod/serv' column"""
        print("[Info]: Making 'prod_serv' column...")
        self.make_new_column({ 'Technology': 'products', 'Service': 'services' }, 
                'technology_service_code', new_colname='prod_serv')
        return
        
    def validate_mapping(self):
        """Validates whether any unmapped data available"""
        self._validate_mapping_technologies()
        self._validate_mapping_segements()
        return

    def _validate_mapping_technologies(self):
        """Validates whether any unmapped data available in technology mapping"""
        mask = pd.isnull(self.df.loc[:, 'arch2'])
        unmapped = self.df.loc[mask, :].shape[0]
        if unmapped > 0:
            print("{} row(s) of unmapped 'internal_sub_business_entity_name' data found".format(unmapped))
            self.df.loc[mask, ['internal_sub_business_entity_name', 'arch2']].to_excel('unmapped_technologies.xlsx', index=False)
        return
        
    def _validate_mapping_segements(self):
        """Validates whether any unmapped data available in segments mapping"""
        mask = pd.isnull(self.df.loc[:, 'rm_name'])
        unmapped = self.df.loc[mask, :].shape[0]
        if unmapped > 0:
            print("{} row(s) of unmapped 'sales_level_5' data found".format(unmapped))
            self.df.loc[mask, ['sales_level_5', 'rm_name']].to_excel('unmapped_segments.xlsx', index=False)
        return
        

class TechSpec1Reader(Reader):
    """Traits contains the common interface of 
    readable functionalities of 'tech_spec1' collection
    """

    mappable_cols = 'internal_sub_business_entity_name'


    def __init__(self, reader):
        """Initializer for TechSpec1Reader class"""
        super().__init__(reader)

    def remove_dedundancy(self):
        """Removes the duplicate entries in the data"""
        self.remove_duplicates(self.mappable_cols)
        return


class CommercialPlan(Reader):
    """Traits contains the common interface of 
    readabve functionalities of 'commercial_plan' collection
    """
    
    def __init__(self, reader):
        """Initializer for CommercialPlan class"""
        super().__init__(reader)
        

class BookingDumpReader(AggregationReader):
    """Traits contains the common interface of 
    readabve functionalities of 'booking_dump' collection
    """
    
    def __init__(self, reader):
        """Initializer for BookingDumpReader class"""
        super().__init__(reader)
        

class SL5ToSegmentsReader(Reader):
    """Traits contains the common interface of 
    readable functionalities of 'sl5_to_segments' collection
    """

    mappable_cols = 'sales_level_5'


    def __init__(self, reader):
        """Initializer for SL5ToSegmentReader class"""
        super().__init__(reader)

    def remove_dedundancy(self):
        """Removes the duplicate entries in the data"""
        self.remove_duplicates(self.mappable_cols)
        return


class MasterUniqueNamesReader(Reader):
    """Traits contains the common interface of 
    readable functionalities of 'master_unique_names' collection
    """

    mappable_cols  = 'customer_name'
    redundant_cols = 'names'
    renamable_cols = { 'names': 'customer_name' }


    def __init__(self, reader):
        """Initializer for TechSpec1Reader class"""
        super().__init__(reader)

    def remove_dedundancy(self):
        """Removes the duplicate entries in the data"""
        self.remove_duplicates(self.redundant_cols)
        return

    def upcase_names(self):
        """Public hook method to change 'names' case into upper"""
        self.upcase_column('names')
        return
        
    def upcase_accnames(self):
        """Public hook method to change 'acc_name' case into upper"""
        self.upcase_column('acc_name')
        return
        
    def upcase_grpnames(self):
        """Public hook method to change 'grp_name' case into upper"""
        self.upcase_column('grp_name')
        return

    def transform_names_to_customername(self):
        """Public method to rename 'names' column into 'customer_name'
        """
        self.rename_columns(self.renamable_cols)
        return
        

class MongoReader():
    """Reads data from MongoDB database
    """
    

    def __init__(self, collname, dbname='ccsdm', host='localhost', port=27017):
        """Initializer for MongoReader class"""
        self.dbname, self.collname = dbname, collname
        self.client                = MongoClient(host, port)
        self.db                    = self.client[dbname]
        self.coll                  = self.db[collname]
        self.hideable              = { '_id': 0, 'timestamp': 0 }

    def read(self, qry):
        """Reads and returns the data as Pandas dataframe"""
        return pd.DataFrame(list(self.coll.find(qry, self.hideable)))

    def agg(self, pipe):
        """Reads and returns the data as Pandas dataframe using aggregation"""
        return pd.DataFrame(list(self.coll.aggregate(pipe)))

    def trash_one(self, qry):
        """Removes just one document from the collection"""
        return self.coll.remove(qry)

    def trash_many(self, qry):
        """Removes as many documents as from the collection"""
        return self.coll.remove(qry, multi=True)

    def read_dict(self, qry={}):
        """Reads and returns the data as python dictionary
        """
        try:
            return list(self.coll.find({}))[0]
        except IndexError:
            raise CollectionDoesNotExistException(self.collname)
        return


class ExcelReader():
    """Reads data from XLSX files
    """


    def __init__(self, filepath, sheetname):
        """Initializer for ExcelReader class"""
        self.filepath  = filepath
        self.sheetname = sheetname

    def read(self, qry=None):
        """Reading data and returns as Pandas dataframe"""
        if self.sheetname is not None:
            xl = pd.ExcelFile(self.filepath)
            return xl.parse(sheetname)
        else:
            return pd.read_excel(self.filepath)

    def validate(self):
        """Validates the Excel file"""
        self._validate_file_existence()
        if self.sheetname is not None:
            self._validate_sheet_existence() 
        return

    def _validate_file_existence(self):
        """Validates the existence of the file and 
        checks if the given path is indeed a file
        """
        if not os.path.exists(self.filepath):
            raise ExcelFileDoesNotExistException(self.filepath)
        return
        
    def _validate_sheet_existence(self):
        """Validates the existence of the sheet"""
        xl = pd.ExcelFile(self.filepath)
        if not self.sheetname in xl.sheet_names:
            raise ExcelSheetDoesNotExistException
        return






