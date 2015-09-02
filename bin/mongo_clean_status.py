#!/usr/bin/env python

# arg parsing related imports
import os
import sys
import utils
from argolog import init_log
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser
from pymongo import MongoClient


def main(args=None):

    # default config
    fn_ar_cfg = "/etc/ar-compute-engine.conf"
    arcomp_conf = "/etc/ar-compute/"
    # Init configuration
    cfg = utils.ArgoConfiguration(fn_ar_cfg)
    cfg.load_tenant_db_conf(os.path.join(arcomp_conf, args.tenant + "_db_conf.json"))
    # Init logging
    log = init_log(cfg.log_mode, cfg.log_file, cfg.log_level, 'argo.job_ar')

    # Split db.collection path strings to obtain database name and collection
    # name

    mongo_host = cfg.get_mongo_host("status")
    mongo_port = cfg.get_mongo_port("status")
    db_status = cfg.get_mongo_database("status")
    col_status = "status_metric"

    # Creating a date integer for use in the database queries
    date_int = int(args.date.replace("-", ""))

    log.info("Connecting to mongo server: %s:%s", mongo_host, mongo_port)
    client = MongoClient(str(mongo_host), int(mongo_port))

    log.info("Opening database: %s", db_status)
    db = client[db_status]

    log.info("Opening collection: %s", col_status)
    col = db[col_status]

    num_of_rows = col.find({"date_integer": date_int}).count()
    log.info("Found %s entries for date %s", num_of_rows, args.date)

    if num_of_rows > 0:
        log.info("Remove entries for date: %s", args.date)
        col.remove({"date_integer": date_int},multi=True)
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
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner ", dest="tenant", metavar="STRING", required="TRUE")

    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
