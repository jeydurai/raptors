#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
from datetime import datetime

from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"



class SyncFilePathNotGivenException(Exception):
    """Neither FilePath Option Nor FileName Argument given Error
    """
    

    def __init__(self, err):
        """Initializer for SyncFilePathNotGivenException class"""
        super().__init__("FilePath Option Or FileName Argument not given")
        self.err = err

    def msg(self):
        """Special Error message
        """
        return err
        
class BothCommAndAllSL3TrueError(Exception):
    """Both 'comm' and 'allsl3' option can not be true
    """
    

    def __init__(self, err):
        """Initializer for BothCommAndAllSL3TrueError class"""
        super().__init__("Option 'comm' and 'allsl3' should be true at the same time")
        self.err = err

    def msg(self):
        """Special Error message
        """
        return err
        
