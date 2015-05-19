#!/usr/bin/env python

# arg parsing related imports
import os
import sys
import json
from argolog import init_log
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser
from pymongo import MongoClient


def main(args=None):

    # default config
    fn_ar_cfg = "/etc/ar-compute-engine.conf"
    arsync_lib = "/var/lib/ar-sync/"

    db_name = "AR"
    col_name = "recalculations"
    
    ArConfig = SafeConfigParser()
    ArConfig.read(fn_ar_cfg)

    # Initialize logging
    log_mode = ArConfig.get('logging', 'log_mode')
    log_file = 'none'

    date_under = args.date.replace("-", "_")

    if log_mode == 'file':
        log_file = ArConfig.get('logging', 'log_file')

    log_level = ArConfig.get('logging', 'log_level')
    log = init_log(log_mode, log_file, log_level, 'argo.mongo_clean_status')

    mongo_host = ArConfig.get('default', 'mongo_host')
    mongo_port = ArConfig.get('default', 'mongo_port')

    # Creating a date integer for use in the database queries
    date_int = int(args.date.replace("-", ""))

    log.info("Connecting to mongo server: %s:%s", mongo_host, mongo_port)
    client = MongoClient(str(mongo_host), int(mongo_port))

    log.info("Opening database: %s", db_name)
    db = client[db_name]

    log.info("Opening collection: %s", col_name)
    col = db[col_name]

    # prepare the query to find requests that include the target date
    query = "'%s' >= this.st.split('T')[0] && '%s' <= this.et.split('T')[0]" % (args.date,args.date)

    results = []
    # run the query 
    for item in col.find({"$where":query},{"_id":0}):
        results.append(item)

    log.info("Date: %s, relevant recomputations found: %s",args.date,len(results))

    # create a temporary recalculation file in the ar-sync folder
    rec_name = "recomputations_" + args.tenant + "_" + date_under + ".json"
    rec_filepath = os.path.join(arsync_lib,rec_name)

    # write output file to the correct job path
    with open(rec_filepath,'w') as output_file:
        json.dump(results,output_file) 


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