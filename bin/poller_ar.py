#!/usr/bin/env python

import sys
from ConfigParser import SafeConfigParser
from utils import ArgoConfiguration
import subprocess
import os
import inspect

from argparse import ArgumentParser

from pymongo import MongoClient

from argolog import init_log

script_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))


def get_mongo_collection(mongo_host, mongo_port, mongo_db):
    """
    :return: pymongo collection object of the recalculations collection
    """
    client = MongoClient(mongo_host, int(mongo_port))
    db = client[mongo_db]
    col = db["recomputations"]
    return col


def get_pending_and_running(col):
    """
    :param col: pymongo collection object
    :return: number of pending requests and running recalculation requests
    """
    num_pen = col.find({"status": "pending"}).count()
    num_run = col.find({"status": "running"}).count()
    return num_pen, num_run


def run_recomputation(col, tenant, num_running, num_pending, threshold):
    """
    Retrieves the first pending recalculation in the db request and queues it for recalculation
    :param col: pymongo collection object
    :param tenant: tenant name
    :param num_running: number of running processes
    :param threshold: threshold number
    """

    # Checks
    if num_pending == 0:
        raise ValueError("Zero pending recomputations")
    elif num_running >= threshold:
        raise ValueError("Over threshold; no recomputation will be executed.")

    pen_recalc = col.find_one({"status": "pending"})
    pen_recalc_id = str(pen_recalc["id"])
    pen_recalc_r = str(pen_recalc["report"])

    # Status update allready implemented in recompute
    # Call recompute execution script
    recompute_script = script_path + "/recompute.py"
    cmd_exec = [recompute_script, "-i", pen_recalc_id, "-t", tenant, "-j", pen_recalc_r]
    # Kickstart executor and continue own execution
    subprocess.Popen(cmd_exec)


def main(tenant=None):
    """
    Checks if there are any pending recomputation requests and if the running
    requests do not exceed a threshold and queues another one to be recomputed
    :param tenant:
    :return:
    """
    # default paths
    fn_ar_cfg = "/etc/ar-compute-engine.conf"
    argo_exec = "/usr/libexec/ar-compute/bin"
    arcomp_conf = "/etc/ar-compute"
    # Init configuration
    cfg = ArgoConfiguration(fn_ar_cfg)
    cfg.load_tenant_db_conf(os.path.join(arcomp_conf, args.tenant + "_db_conf.json"))
    threshold = cfg.threshold

    # Init logging
    log = init_log(cfg.log_mode, cfg.log_file, cfg.log_level, 'argo.recompute')
    db_name = cfg.get_mongo_database("ar")

    col = get_mongo_collection(cfg.mongo_host, cfg.mongo_port, db_name)
    num_pen, num_run = get_pending_and_running(col)
    log.info("Running recomputations: %s (threshold: %s)", num_run, threshold)
    try:
        run_recomputation(col, tenant, num_run, num_pen, threshold)
        log.info("Below threshold recomputation sent for execution")
    except ValueError as ex:
        log.info(ex)


if __name__ == "__main__":
    # Feed Argument parser with the description
    # No arguments needed
    arg_parser = ArgumentParser(
        description="Polling for pending recomputations requests")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner", type=str, dest="tenant", metavar="STRING",
        required="TRUE")
    # Parse the command line arguments accordingly and introduce them to
    # main...
    args = arg_parser.parse_args()
    sys.exit(main(tenant=args.tenant))
