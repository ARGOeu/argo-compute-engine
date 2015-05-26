#!/usr/bin/env python

# arg parsing related imports
import os
import sys
import json
from argorun import run_cmd
from argolog import init_log
from argparse import ArgumentParser
from utils import ArgoConfiguration
from utils import get_date_range
from utils import get_date_str


def do_recompute(argo_exec,date,tenant,job,log):
    log.info("Recomputing: for tenant: %s and job: %s and date: %s", tenant, job, date)
    cmd_job_ar = [os.path.join(argo_exec, "job_ar.py"), '-d', date, '-t', tenant, '-j', job]
    #run_cmd(cmd_job_ar, log)

def loop_recompute(argo_exec,date_range,tenant,job_set,log):

    for dt in date_range:
        date_arg = get_date_str(dt)
        for job in job_set:
            do_recompute(argo_exec,date_arg,tenant,job,log)

def main(args=None):

    # default paths
    fn_ar_cfg = "/etc/ar-compute-engine.conf"
    arsync_lib = "/var/lib/ar-sync/"
    argo_exec = "/usr/libexec/ar-compute/bin"
    
    # Init configuration
    cfg = ArgoConfiguration(fn_ar_cfg)

    # Init logging
    log = init_log(cfg.log_mode, cfg.log_file, cfg.log_level, 'argo.recompute')
    job_set = ['Critical','CloudMon']
    dates = get_date_range(args.start_date,args.end_date)
    loop_recompute(argo_exec,dates,args.tenant,job_set,log)   


if __name__ == "__main__":

    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(description="Execute Recomputations for a period of time (start,end)")
    arg_parser.add_argument(
        "-i", "--id", help="recomputation id", dest="id", metavar="STRING", required="TRUE")
    arg_parser.add_argument(
        "-s", "--start-date", help="start date", dest="start_date", metavar="DATE", required="TRUE")
    arg_parser.add_argument(
        "-e", "--end-date", help="end date", dest="end_date", metavar="DATE", required="TRUE")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner", dest="tenant", metavar="STRING", required="TRUE")
    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))