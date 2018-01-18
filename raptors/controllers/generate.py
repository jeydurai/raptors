#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
import re
from datetime import datetime
from pprint import pprint
from raptors.models.readers import BookingDumpReader, SFDCDumpReader, MongoReader
from raptors.models.writers import Writer, MongoWriter, ExcelWriter
from raptors.helpers.raptortools import ParsingTool as PT
from raptors.helpers.mongoutils import Mongo
from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class Generate():
    """
    Generates various reports in excel format based on 
    name and owner
    """


    def __init__(self, rept_opts, mong_opts, xl_opts):
        """Initializer for Generate class"""
        self.name         = rept_opts['name']
        self.owner        = rept_opts['owner']
        self.history      = rept_opts['history']
        self.cur_year     = rept_opts['cur_year']
        self.field_config = rept_opts['field_config']
        self.mong_opts    = mong_opts
        self.xl_opts      = xl_opts
        self.generator    = None
        
    def execute(self):
        """Hook method to run specific generator's execute method"""
        self._set_generator()
        self.generator.set_field_config()
        self.generator.set_query_config()
        self.generator.read()
        self.generator.expunge_all_existing_data()
        self.generator.write()
        return

    def _set_generator(self):
        """Sets the type of generator based on 'name'"""
        if self.name.lower() == 'booking':
            self.generator = BookingGenerator(mong_opts=self.mong_opts, owner=self.owner, 
                    history=self.history, cur_yr=self.cur_year, xl_opts=self.xl_opts, field_config=self.field_config)
        elif self.name.lower() == 'sfdc':
            self.generator = SFDCGenerator(mong_opts=self.mong_opts, owner=self.owner, 
                    history=self.history, cur_yr=self.cur_year, xl_opts=self.xl_opts, field_config=self.field_config)
        else:
            pass
        return
        
        
class Generator():
    """Parent class for the generators
    """


    def __init__(self, owner='', history='', cur_yr='', xl_opts='', field_config='', mong_opts='', **kwargs):
        """Initializer for Generator class"""
        super().__init__(**kwargs)
        self.owner        = owner.lower()
        self.history      = history if history else 1
        self.cur_yr       = cur_yr if cur_yr else 2018
        self.filepath     = os.path.expanduser(xl_opts['filepath'])
        self.sheetname    = xl_opts['sheetname']
        self.field_config = field_config
        self.dbname       = mong_opts['dbname'] if mong_opts['dbname'] else 'ccsdm'
        self.host         = mong_opts['host'] if mong_opts['host'] else 'localhost'
        self.port         = mong_opts['port'] if mong_opts['port'] else 27017
        self.months       = self._get_stringified_months()
        self.fin_months   = self._get_fin_months()
        self.qconfig      = {}
        self.aggpipes     = []
        self.xl_writer    = None
        self.mong_writer  = None
        self.uniq_fields  = { 'fiscal_year_id', 'fiscal_quarter_id', 'fiscal_period_id', 'fiscal_month_id', 
                'fiscal_week_id', 'sales_level_4', 'sales_level_5', 'sales_level_6', 'rm_name', 'od_name', 
                'segment', 'country', 'region', 'state', 'prod_serv', 'arch1', 'arch2', 'tech_name1', 
                'tech_name2', 'tech_name3' }
        self.val_fields = set()

    def expunge_all_existing_data(self):
        """Private method to clean up All existing data"""
        self._warn_user()
        self._expunge()
        return

    def _warn_user(self):
        """Warns the user with the hazardous deletion process"""
        recs_planned = self.mong_writer.how_many_docs({})
        print("[Info]: {} row(s) existing! {} row(s) planned for trashing!?!".format(
            self.mong_writer.recs_before(), recs_planned))
        return

    def _expunge(self):
        """Executes the removing of documents from the writable collection"""
        self.mong_writer.trash_many({})
        return

    def _get_stringified_months(self):
        """Prepares Stringified Months"""
        months = []
        for i in range(1, 13):
            months.append(str(i).zfill(2))
        return months

    def _get_fin_months(self):
        """Sets the fin_months based on the current year & history"""
        years = []
        for h in range(self.history):
            years.append(self.cur_yr-h)
        return [ str(y) + m for y in years for m in self.months ]

    def set_query_config(self):
        """Sets the Query Configuration based on owner"""
        self._set_query_config_for_owner()
        return

    def _set_query_config_for_owner(self):
        """Sets the Owner specific query configuration"""
        if (self.owner == 'sudhir' or self.owner == 'comm' or self.owner == 'commercial' or self.owner == 'nayar'):
            self.qconfig['sales_level_3'] = ['INDIA_COMM_1']
        elif (self.owner == 'mukund' or self.owner == 'mukundhan' or self.owner == 'sw_geo' or self.owner == 'sw-geo'):
            self.qconfig['sales_level_4'] = ['INDIA_COMM_SW_GEO']
        elif (self.owner == 'tm' or self.owner == 'tirthankar' or self.owner == 'sl_tl' or self.owner == 'sl-tl'):
            self.qconfig['sales_level_4'] = ['INDIA_COMM_SL_TL']
        elif (self.owner == 'vipul' or self.owner == 'ne_geo' or self.owner == 'ne-geo'):
            self.qconfig['sales_level_4'] = ['INDIA_COMM_NE_GEO']
        elif (self.owner == 'fakhrudhin' or self.owner == 'bd' or self.owner == 'bangladesh'):
            self.qconfig['sales_level_4'] = ['INDIA_COMM_BD']
        return


class BookingGenerator(Generator):
    """BookingDump Generator
    """

    collname        = 'booking_dump'
    summarycollname = 'booking_dump_summary'

    def __init__(self, **kwargs):
        """Initializer for Booking Generator"""
        super().__init__(**kwargs)
        self.filename     = os.path.join(self.filepath, PT.timestamped_filename(self.owner + '_' + self.collname, '.xlsx'))
        self.reader       = BookingDumpReader(MongoReader(self.collname, dbname=self.dbname, 
            host=self.host, port=self.port))
        self.xl_writer    = Writer(ExcelWriter(self.filename, self.sheetname))
        self.mong_writer  = Writer(MongoWriter(self.dbname, self.summarycollname, host=self.host, port=self.port))
        self.__all_fields = None
        self.uniq_fields = list(self.uniq_fields.union({ 'sales_agent', 'recurring_offer_flag', 'tier_code', 
            'grp_ver', 'grp_ver2', 'product_classification', 'grp_name' }))
        self.val_fields = list(self.val_fields.union({ 'booking_net', 'base_list', 'standard_cost' }))

    def read(self):
        """Public method to read data"""
        print("\nReading Data...")
        self.reader.read(self.aggpipes)
        return

    def write(self):
        """Public method to write the data read"""
        print("Data will be written to {}".format(self.filename))
        self.xl_writer.write(self.reader.df)
        print("Data is now being written to {}.{} collection in MongoDB".format(self.collname, self.summarycollname))
        self.mong_writer.write(self.reader.df)
        return

    @property
    def all_fields(self):
        """All Unique Fields Getter method"""
        return self.__all_fields

    def set_query_config(self):
        """Sets the Query Configuration based on owner"""
        super()._set_query_config_for_owner()
        self._set_query_config_for_period()
        return

    def _set_query_config_for_period(self):
        """Sets the 'fiscal_period_id' specific query configuration"""
        for month in self.fin_months:
            self.qconfig['fiscal_period_id'] = [ int(month) ]
            qry = [ { '$match': Mongo.make_query(self.qconfig) }, 
                    { '$group': Mongo.make_group(self.uniq_fields, self.val_fields) }, 
                    { '$project': Mongo.make_project(self.uniq_fields, self.val_fields) }]
            self.aggpipes.append(qry)
        return

    def set_field_config(self):
        """Validates and sets teh configuration for field addition/removal"""
        configs = PT.parse_field_config(self.field_config)
        for config in configs:
            if config.switch == 'a':
                self.uniq_fields.append(config.field)
            elif config.switch == 'r':
                if config.field in self.uniq_fields:
                    self.uniq_fields.remove(config.field)
                elif config.field in self.val_fields:
                    self.val_fields.remove(config.field)
                else:
                    pass
        self.__all_fields = []
        self.__all_fields.extend(self.uniq_fields)
        self.__all_fields.extend(self.val_fields)
        return


class SFDCGenerator(Generator):

    collname         = 'sfdc_dump'
    cleaned_collname = 'sfdc_dump_cleaned'

    def __init__(self, **kwargs):
        """Initializer for SFDC Generator"""
        super().__init__(**kwargs)
        self.filename     = os.path.join(self.filepath, PT.timestamped_filename(self.owner + '_' + self.collname, '.xlsx'))
        self.reader       = SFDCDumpReader(MongoReader(self.collname, dbname=self.dbname, 
            host=self.host, port=self.port))
        self.xl_writer    = Writer(ExcelWriter(self.filename, self.sheetname))
        self.mong_writer  = Writer(MongoWriter(self.dbname, self.cleaned_collname, host=self.host, port=self.port))
        self.sl3          = 'INDIA_COMM_1'

    def read(self):
        """Public method to read data"""
        print("\nReading Data...")
        # pprint(self.aggpipes[0])
        self._read_dump() 
        return

    def _read_dump(self):
        """Private method to read and store the dump data"""
        print("[Info]: Reading 'sfdc_raw_dump`' data")
        qry = { 
            'sales_level_3': self.sl3, 'opportunity_status': 'Active', 
            '$or': [ { 'past_due': 'FALSE' }, { 'past_due': 'NEGATIVE' } ] 
        }
        self.reader.read(qry=qry)
        return

    def write(self):
        """Public method to write the data read"""
        print("Data will be written to {}".format(self.filename))
        self.xl_writer.write(self.reader.df)
        print("Data will be written to {}.{} collection".format(self.dbname, self.cleaned_collname))
        self.mong_writer.write(self.reader.df)
        return



