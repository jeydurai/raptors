#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

import sys
import click
import logging
from .controllers.backup import Backup

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
    """Entry point for 'raptors' application
    """
    full_decor = "="*80
    half_decor = "="*35
    click.echo("")
    click.echo(full_decor)
    click.echo( "{} Raptors  {}".format( half_decor, half_decor))
    click.echo(full_decor)

@main.command()
@click.option(
    '--alldata/--no-alldata',
    default=False,
    help='Backup to be done on all files or just MongoDB')
@click.option(
    '--outfile', '-o',
    help='Outfile location in which dump should be created')
@click.option(
    '--database', '-d',
    help='What database to be backed up')
@click.option(
    '--collection', '-c',
    help='What collection to be backed up')
@pass_config
def backup(config, alldata, outfile, database, collection):
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


if __name__ == "__main__":
        main()

