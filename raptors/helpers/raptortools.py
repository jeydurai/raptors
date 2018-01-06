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


        

