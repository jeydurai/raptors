#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os

from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class Mongo():
    """MongoDB Query components generators
    """


    @staticmethod
    def make_OR(array, on_field):
        """Makes OR Query from the array for the field name given"""
        or_query = []
        for elem in array:
            obj = {on_field: elem}
            or_query.append(obj)
        return {'$or': or_query}

    @staticmethod
    def make_query(qry_config):
        """Makes Query object based on configuration"""
        qry = { '$and': [] }
        for field, config in qry_config.items():
            if not isinstance(config, list):
                qry['$and'].append({ field: config })
            else:
                qry['$and'].append(Mongo.make_OR(config, field))
        return qry

    @staticmethod
    def make_group(uniq_fields, val_fields):
        """Makes Group object for the aggregation pipe"""
        uniq_grp = Mongo._make_uniq_fields_group(uniq_fields)
        val_grp  = Mongo._make_val_fields_group(val_fields)
        return { **uniq_grp, **val_grp }
    
    @staticmethod
    def make_project(uniq_fields, val_fields):
        """Makes Project object for the aggregation pipe"""
        obj = { '_id': 0 }
        uniq_grp = Mongo._make_uniq_fields_project(uniq_fields)
        val_grp  = Mongo._make_val_fields_project(val_fields)
        return { **obj, **uniq_grp, **val_grp }
    
    @classmethod
    def _make_uniq_fields_group(cls, fields):
        """Makes group object from unique fields"""
        obj = { '_id': {} }
        for field in fields:
            obj['_id'][field] = '$' + field
        return obj
       
    @classmethod
    def _make_val_fields_group(cls, fields):
        """Makes group object from value fields"""
        obj = {}
        for field in fields:
            obj[field] = { '$sum': '$' + field }
        return obj
       
    @classmethod
    def _make_uniq_fields_project(cls, fields):
        """Makes project object from unique fields"""
        obj = {}
        for field in fields:
            obj[field] = '$_id.' + field
        return obj
       
    @classmethod
    def _make_val_fields_project(cls, fields):
        """Makes project object from value fields"""
        obj = {}
        for field in fields:
            obj[field] = '$' + field
        return obj
       















