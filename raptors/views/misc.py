#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import os
from datetime import datetime
import timeit

from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


class ProgressBar():
    """Generates the progress bar and other 
    related functionalities
    """


    idx = 0

    def __init__(self, tot_rows, tic, freq=10):
        """Initializer for ProgressBar class"""
        self.tot_rows = tot_rows
        self.tic      = tic
        self.freq     = freq

    def display(self, idx):
        """Prints Progress"""
        if (ProgressBar.idx+1) % 10 == 0:
            toc = timeit.default_timer()
            elapsed = toc - self.tic
            el_in_hr = int(float(elapsed)/float(60*60))
            el_in_min = int(float(elapsed)/float(60))
            el_in_sec = elapsed % (60*60)
            completed = (float(ProgressBar.idx+1)/float(self.tot_rows))*100
            sys.stdout.write( "Processed %.2f%% - (%d)/(%d) row(s) %d hr(s) %d min(s) %d sec(s)...\r" \
                    % (completed, ProgressBar.idx+1, self.tot_rows, el_in_hr, el_in_min, el_in_sec))
            sys.stdout.flush()
        ProgressBar.idx += 1
        return
        
