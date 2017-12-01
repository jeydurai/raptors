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
            obj = {"fiscal_week_id": elem}
            or_query.append(obj)
        return {'$or': or_query}

        
