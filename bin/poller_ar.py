#!/usr/bin/env python

import sys
from ConfigParser import SafeConfigParser

from argparse import ArgumentParser

from pymongo import MongoClient

from bson.objectid import ObjectId

from argolog import init_log


def main(args=None):
    # default config
    fn_ar_cfg = "/etc/ar-compute-engine.conf"

    ArConfig = SafeConfigParser()
    ArConfig.read(fn_ar_cfg)

    # Initialize logging
    log_mode = ArConfig.get('logging', 'log_mode')
    log_file = 'none'

    if log_mode == 'file':
        log_file = ArConfig.get('logging', 'log_file')

    log_level = ArConfig.get('logging', 'log_level')
    log = init_log(log_mode, log_file, log_level, 'argo.poller')

    mongo_host = ArConfig.get('default', 'mongo_host')
    mongo_port = ArConfig.get('default', 'mongo_port')

    threshold = int(ArConfig.get('default', 'recomp_threshold'))
    log.info("Threshold: %s", threshold)

    client = MongoClient(mongo_host, int(mongo_port))
    db = client["AR"]
    col = db["recalculations"]
    num_pen = col.find({"s": "pending"}).count()
    num_run = col.find({"s": "running"}).count()
    log.info("Running recalculations: %s (threshold: %s)", num_run, threshold)

    if num_pen == 0:
        log.info("No pending recomputations present")

    elif num_run < threshold:
        log.info("Below threshold; recomputation is about to be executed...")
        pen_recalc = col.find_one({"s": "pending"})
        pen_recalc_id = str(pen_recalc["_id"])
        col.update({"_id": ObjectId(pen_recalc_id)}, {"$set": {"s": "running"}})

        # TODO: placeholder: call recomputation executor.

    else:
        log.info("Over threshold; no recomputation will be executed.")


if __name__ == "__main__":
    # Feed Argument parser with the description
    # No arguments needed
    arg_parser = ArgumentParser(
        description="Polling for pending recomputations requests")

    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))