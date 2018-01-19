#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
import re
from datetime import datetime
from collections import namedtuple
from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class GeneralTool():
    """Contains the tools to provide DRY
    """

    @staticmethod
    def get_query_config_for_owner(owner):
        """Sets the Owner specific query configuration"""
        qconfig = {}
        if (owner == 'sudhir' or owner == 'comm' or owner == 'commercial' or owner == 'nayar'):
            qconfig['sales_level_3'] = ['INDIA_COMM_1']
        elif (owner == 'mukund' or owner == 'mukundhan' or owner == 'sw_geo' or owner == 'sw-geo'):
            qconfig['sales_level_4'] = ['INDIA_COMM_SW_GEO']
        elif (owner == 'tm' or owner == 'tirthankar' or owner == 'sl_tl' or owner == 'sl-tl'):
            qconfig['sales_level_4'] = ['INDIA_COMM_SL_TL']
        elif (owner == 'vipul' or owner == 'ne_geo' or owner == 'ne-geo'):
            qconfig['sales_level_4'] = ['INDIA_COMM_NE_GEO']
        elif (owner == 'fakhruddhin' or owner == 'bd' or owner == 'bangladesh'):
            qconfig['sales_level_4'] = ['INDIA_COMM_BD']
        return config


class ParsingTool():
    """Contains parsing tools to handle
    Strings with easiness
    """


    @staticmethod
    def parse_field_config(config_args):
        """Static Method to parse field configuration 
        from a tuple of arguments
        """
        FieldConfig = namedtuple('FieldConfig', [ 'switch', 'field' ])
        configs = []
        for i, c in enumerate(config_args):
            parsed_config = re.split(':\s*?|,\s*?|\|\s*?', c)
            configs.append(FieldConfig(switch=parsed_config[0].lower(), field=parsed_config[1]))
        return configs

    @staticmethod
    def time_string(fmt=None):
        """Returns the Stringified Timestamp"""
        if fmt is None:
            return datetime.now().strftime('%d-%m-%Y-%H%M')
        return datetime.now().strftime(fmt)
        
    @staticmethod
    def timestamped_filename(prefix, extn):
        """Returns the filename with timestamp"""
        return "{}_{}{}".format(prefix, ParsingTool.time_string(), extn)


        

