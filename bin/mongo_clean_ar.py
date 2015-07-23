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

    mongo_host = cfg.get_mongo_host("ar")
    mongo_port = cfg.get_mongo_port("ar")
    db_name = cfg.get_mongo_database("ar")

    col_service = "service_ar"
    col_egroup = "endpoint_group_ar"

    # Create a date integer for use in the database queries
    date_int = int(args.date.replace("-", ""))

    log.info("Connecting to mongo server: %s:%s", mongo_host, mongo_port)
    client = MongoClient(str(mongo_host), int(mongo_port))

    # for service collection cleanup do the following
    log.info("Regarding service a/r data...")

    log.info("Opening database: %s", db_name)
    db = client[db_name]

    log.info("Opening collection: %s", col_service)
    col = db[col_service]

    if args.report:
        num_of_rows = col.find({"date": date_int, "report": args.report}).count()
        log.info("Found %s entries for date %s and report %s",
                 num_of_rows, args.date, args.report)
    else:
        num_of_rows = col.find({"date": date_int}).count()
        log.info("Found %s entries for date %s", num_of_rows, args.date)

    if num_of_rows > 0:

        if args.report:
            log.info(
                "Remove entries for date: %s and report: %s", args.date, args.report)
            col.delete_many({"date": date_int, "report": args.report})
        else:
            log.info("Remove entries for date: %s", args.date)
            col.delete_many({"date": date_int})

        log.info("Entries Removed!")

    else:
        log.info("Zero entries found. No need to remove anything")

    # for service collection cleanup do the following
    log.info("Regarding endpoint group a/r data...")

    log.info("Opening database: %s", db_name)
    db = client[db_name]

    log.info("Opening collection: %s", col_egroup)
    col = db[col_egroup]

    if args.report:
        num_of_rows = col.find({"date": date_int, "report": args.report}).count()
        log.info("Found %s entries for date %s and report %s",
                 num_of_rows, args.date, args.report)
    else:
        num_of_rows = col.find({"date": date_int}).count()
        log.info("Found %s entries for date %s", num_of_rows, args.date)

    if num_of_rows > 0:

        if args.report:
            log.info(
                "Remove entries for date: %s and report: %s", args.date, args.report)
            col.delete_many({"date": date_int, "report": args.report})
        else:
            log.info("Remove entries for date: %s", args.date)
            col.delete_many({"date": date_int})

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
    arg_parser.add_argument(
        "-r", "--report", help="report", dest="report", metavar="STRING")

    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
