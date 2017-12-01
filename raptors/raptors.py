#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import click
import os
import logging
from raptors.controllers.backup import Backup
from raptors.controllers.sync import Sync
from raptors.controllers.cleandump import CleanBookingDump, CleanSFDCDump
from raptors.helpers.exceptions.controllersexceptions import SyncFilePathNotGivenException
from raptors.helpers.exceptions.controllersexceptions import BothCommAndAllSL3TrueError

from raptors import __version__

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"

_logger = logging.getLogger(__name__)

class Config(object):
    """Configuration class to connect 
        group command and sub commands
    """
    def __init__(self):
        self.homedir = '~/Documents/Jeyaraj/My_Contents/Work/Commercial/Templates/XLSX'


pass_config = click.make_pass_decorator(Config, ensure=True)


@click.group()
@pass_config
def main(config):
    """Data 'raptors' welcomes you!
    """
    full_decor = "="*80
    half_decor = "="*35
    click.echo("")
    click.echo(full_decor)
    click.echo( "{} Raptors  {}".format( half_decor, half_decor))
    click.echo(full_decor)

@main.command()
@click.option( '--alldata/--no-alldata', default=False, help='Backup to be done on all files or just MongoDB')
@click.option( '--outfile', '-o', help='Outfile location in which dump should be created')
@click.option( '--database', '-d', help='What database to be backed up')
@click.option( '--collection', '-c', help='What collection to be backed up')
@pass_config
def clonedata(config, alldata, outfile, database, collection):
    """Helps to backup Data
    """
    opts = {
            'all'      : alldata,
            'out'      : outfile,
            'db'       : database,
            'coll'     : collection,
            'home_dir' : config.homedir,
    }
    Backup(opts).execute()
    return

@main.command()
@click.option('--filepath',   '-f', help='Absolute file path to read the data')
@click.option('--sheetname',  '-s', help='Excel file sheetname of the data')
@click.option('--collection', '-c', help='MongoDB switch to give collection name')
@click.option('--database',   '-d', help='MongoDB switch to give database name')
@click.option('--host',       '-h', help='MongoDB Host')
@click.option('--port',       '-p', help='MongoDB Port')
@click.argument('filename')
@pass_config
def takefood(config, filepath, sheetname, collection, database, host, port, filename):
    """Data Uploader into Database from Excel"""
    if filepath is None and filename is None:
        raise SyncFilePathNotGivenException("")
    org_db = filepath
    if not org_db:
        org_db  = os.path.join(os.path.expanduser(config.homedir), "{}.xlsx".format(filename))
    org_db = org_db if org_db.lower().endswith('.xlsx') else "{}.xlsx".format(org_db)
    org_tbl = sheetname if sheetname else None
    des_db  = database if database else 'ccsdm'
    des_tbl = collection if collection else filename
    Sync(org_db, org_tbl, des_db, des_tbl, host=host, port=port).execute()
    return

@main.command()
@click.option('--history', type=int, help='Number of years for which validation to be done')
@click.option( '--comm/--no-comm', default=True, help='Commercial or All others with Commercial')
@click.option('--collection', '-c', help='MongoDB switch to give collection name')
@click.option('--database',   '-d', help='MongoDB switch to give database name')
@click.option('--host',       '-h', help='MongoDB Host')
@click.option('--port',       '-p', help='MongoDB Port')
@click.argument('years', nargs=-1, required=False)
@pass_config
def makepacks(config, history, comm, collection, database, host, port, years):
    """Validates and creates 'booking_dump' collection"""
    des_db  = database if database else 'ccsdm'
    des_tbl = collection if collection else 'booking_dump'
    CleanBookingDump(history, years, comm, des_tbl, des_db, host=host, port=port).execute()
    return
    
@main.command()
@click.option( '--comm/--no-comm', default=True, help='Commercial or All others with Commercial')
@click.option('--collection', '-c', help='MongoDB switch to give collection name')
@click.option('--database',   '-d', help='MongoDB switch to give database name')
@click.option('--host',       '-h', help='MongoDB Host')
@click.option('--port',       '-p', help='MongoDB Port')
@pass_config
def migratefuture(config, comm, collection, database, host, port):
    """Validates and creates 'sfdc_dump' collection"""
    des_db  = database if database else 'ccsdm'
    des_tbl = collection if collection else 'sfdc_dump'
    CleanSFDCDump(comm, des_tbl, des_db, host=host, port=port).execute()
    return
    
    
if __name__ == "__main__":
        main()

