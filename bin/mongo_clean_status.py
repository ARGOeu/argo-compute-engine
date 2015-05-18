#!/usr/bin/env python

# arg parsing related imports
import os
import sys
from argolog import init_log
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser
from pymongo import MongoClient


def main(args=None):

    # default config
    fn_ar_cfg = "/etc/ar-compute-engine.conf"

    ArConfig = SafeConfigParser()
    ArConfig.read(fn_ar_cfg)

    # Initialize logging
    log_mode = ArConfig.get('logging', 'log_mode')
    log_file = None

    if log_mode == 'file':
        log_file = ArConfig.get('logging', 'log_file')

    log_level = ArConfig.get('logging', 'log_level')
    log = init_log(log_mode, log_file, log_level, 'argo.mongo_clean_status')

    mongo_host = ArConfig.get('default', 'mongo_host')
    mongo_port = ArConfig.get('default', 'mongo_port')

    mongo_dest = ArConfig.get('datastore_mapping', 'sdetail_dest')

    # Split db.collection path string to obtain database name and collection
    # name
    mongo_dest = mongo_dest.split('.')
    db_status = mongo_dest[0]
    col_status = mongo_dest[1]

    # Creating a date integer for use in the database queries
    date_int = int(args.date.replace("-", ""))

    log.info("Connecting to mongo server: %s:%s", mongo_host, mongo_port)
    client = MongoClient(str(mongo_host), int(mongo_port))

    log.info("Opening database: %s", db_status)
    db = client[db_status]

    log.info("Opening collection: %s", col_status)
    col = db[col_status]

    num_of_rows = col.find({"di": date_int}).count()
    log.info("Found %s entries for date %s", num_of_rows, args.date)

    if num_of_rows > 0:
        log.info("Remove entries for date: %s", args.date)
        col.remove({"di": date_int})
        log.info("Entries Removed!")
    else:
        log.info("Zero entries found. No need to remove anything")

if __name__ == "__main__":

    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(
        description="clean status detail data for a day")
    arg_parser.add_argument(
        "-d", "--date", help="date", dest="date", metavar="DATE", required="TRUE")

    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
