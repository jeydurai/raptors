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


class Prepare():
    """Prepare commands functionalities
    """


    def __init__(self, command, opts):
        """Initializer of Fetch class"""
        self.cmd  = command
        self.opts = opts

    def execute(self):
        """Public method to execute the command
        """
        return


        
        
