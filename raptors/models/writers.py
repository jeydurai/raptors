#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
from datetime import datetime
from pymongo import MongoClient
from raptors.views.misc import ProgressBar
import timeit

from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class Writer():
    """Traits contains the common interface of 
    readable functionalities
    """


    def __init__(self, writer):
        """Initializer for Writable class"""
        self.writer = writer

    def write(self, data):
        """Hook method to write the data irrespective of the engine
        as abstracted interface
        """
        self.writer.write(data)
        return

    def trash_one(self, qry):
        """Hook method to remove just one document/row 
        irrespective of the engine as abstracted interface
        """
        return self.writer.trash_one(qry)

    def trash_many(self, qry):
        """Hook method to remove many documents/rows
        irrespective of the engine as abstracted interface
        """
        return self.writer.trash_many(qry)

    def validate(self):
        """Hook method to validate the specific writer 
        """
        self.writer.validate()
        return

    def how_many_docs(self, qry):
        """Hook method to return number of documents
        """
        print("Query {}".format(qry))
        return self.writer.how_many_docs(qry)

    def recs_before(self):
        """Hook method to return number of documents before
        """
        return self.writer.recs_before

    def recs_after(self):
        """Hook method to return number of documents after
        """
        return self.writer.recs_after

    def recs_planned(self):
        """Hook method to return number of documents planned
        """
        return self.writer.recs_planned


class MongoWriter():
    """Writes Dictionary data into MongoDB
    """


    def __init__(self, dbname, collname, host='localhost', port=27017):
        """Initializer for MongoWriter"""
        self.dbname, self.collname = dbname, collname
        self.client                = MongoClient(host, port)
        self.db                    = self.client[dbname]
        self.coll                  = self.db[collname]
        self.sensitive_collections = [ 'ent_dump_from_finance', 'ent_dump_from_finance_old' ]
        self.alert                 = collname in self.sensitive_collections
        self.recs_before           = self.how_many_docs()
        self.recs_after            = 0
        self.recs_planned          = 0

    def write(self, df):
        """Public method to write the data into MongoDB"""
        tot_rows = df.shape[0]
        print('[Info]: Writing Data...')
        tic  = timeit.default_timer()
        pbar = ProgressBar(tot_rows, tic)
        for idx, row in df.iterrows():
            try:
                result = self.coll.insert(dict(row))
            except OverflowError:
                opp_id = row['opportunity_id']
                row['opportunity_id'] = str(opp_id) if isinstance(opp_id, int) else opp_id
                result = self.coll.insert(dict(row))
            pbar.display(idx)
        tot_docs = self.how_many_docs()
        print("Collection now has {} document(s)\n\nAll done!".format(tot_docs))
        return

    def validate(self):
        pass
        
    def how_many_docs(self, qry={}):
        """Returns the number of documents existing in the collection"""
        return self.coll.find(qry).count()

    def trash_one(self, qry):
        """Removes just one document from the collection"""
        self.recs_planned = self.how_many_docs(qry)
        self.coll.remove(qry)
        self.recs_after  = self.how_many_docs()

    def trash_many(self, qry):
        """Removes as many documents as from the collection"""
        self.recs_planned = self.how_many_docs(qry)
        self.coll.remove(qry, multi=True)
        self.recs_after  = self.how_many_docs()
        



        










        
        
