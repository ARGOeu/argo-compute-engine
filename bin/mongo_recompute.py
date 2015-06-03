#!/usr/bin/env python

# arg parsing related imports
import os
import sys
import json
from argolog import init_log
from argparse import ArgumentParser
from utils import ArgoConfiguration
from utils import get_date_under
from pymongo import MongoClient


def write_output(results, tenant, date_under, arsync_lib):
    """
    Write the json array containing recomputation information to a disk file

    :param results: recomputation results in json format
    :param tenant: the name of the tenant (used in filename)
    :param date_under: target date formatted with underscores (used in filename)
    :param arsync_lib: path to the arsync root directory  
    """
    # create a temporary recalculation file in the ar-sync folder
    rec_name = "recomputations_" + tenant + "_" + date_under + ".json"
    rec_filepath = os.path.join(arsync_lib, rec_name)

    # write output file to the correct job path
    with open(rec_filepath, 'w') as output_file:
        json.dump(results, output_file)


def get_mongo_collection(mongo_host, mongo_port, db, collection, log):
    """
    Returns pymongo object containing reference to the recalculation collection
    in the datastore (MongoDB)

    :param mongo_host: hostname of MongoDB server
    :param mongo_port: port number of MongoDB server
    :param db: database name to use
    :param collection: collection name to open
    :param log: logger object
    :returns: pymongo collection object
    """
    # Connect to the mongo server (host,port)
    log.info("Connecting to mongo server: %s:%s", mongo_host, mongo_port)
    client = MongoClient(str(mongo_host), int(mongo_port))
    # Connect to the database
    log.info("Opening database: %s", db)
    db = client[db]
    log.info("Opening collection: %s", collection)
    col = db[collection]

    return col


def get_mongo_results(collection, date):
    """
    Returns recomputation results for a specific date by querying MongoDB Datastore

    :param collection: target datastore collection containing recomputations
    :param date: specific date to query
    :returns: json object containing results
    """
    # Init results list
    results = []
    # prepare the query to find requests that include the target date
    query = "'%s' >= this.st.split('T')[0] && '%s' <= this.et.split('T')[0]" % (date, date)
    # run the query
    for item in collection.find({"$where": query}, {"_id": 0}):
        results.append(item)

    return results


def main(args=None):
    """
    Script to retrieve relevant recomputations requests for a specific date
    
    :param args: Command line arguments
    """

    # default paths
    fn_ar_cfg = "/etc/ar-compute-engine.conf"
    arsync_lib = "/var/lib/ar-sync/"

    # Init configuration
    cfg = ArgoConfiguration(fn_ar_cfg)
    db_name = "AR"
    col_recomputations = "recalculations"

    # Init logging
    log = init_log(cfg.log_mode, cfg.log_file, cfg.log_level, 'argo.mongo_recompute')

    # Get mongo collection
    col = get_mongo_collection(cfg.mongo_host, cfg.mongo_port, db_name, col_recomputations, log)
    results = get_mongo_results(col, args.date)
    log.info("Date: %s, relevant recomputations found: %s", args.date, len(results))

    # Write results to file
    write_output(results, args.tenant, get_date_under(args.date), arsync_lib)


if __name__ == "__main__":

    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(description="Get relevant recomputation requests")
    arg_parser.add_argument(
        "-d", "--date", help="date", dest="date", metavar="DATE", required="TRUE")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner ", dest="tenant", metavar="STRING", required="TRUE")
    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
