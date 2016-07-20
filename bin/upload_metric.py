#!/usr/bin/env python

# arg parsing related imports
import os
import sys
from argolog import init_log
from argorun import run_cmd
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser


def main(args=None):

    # default config
    fn_ar_cfg = "/etc/ar-compute-engine.conf"
    date_under = args.date.replace("-", "_")

    ArConfig = SafeConfigParser()
    ArConfig.read(fn_ar_cfg)

    # Get sync exec and path
    arsync_exec = ArConfig.get('connectors', 'sync_exec')
    arsync_lib = ArConfig.get('connectors', 'sync_path')
    consumers_root = ArConfig.get('consumers','consumers_root')

    # Get mode from config file
    ar_mode = ArConfig.get('default', 'mode')



    prefilter_clean = ArConfig.get('default', 'prefilter_clean')

    # Initialize logging
    log_mode = ArConfig.get('logging', 'log_mode')
    log_file = None

    if log_mode == 'file':
        log_file = ArConfig.get('logging', 'log_file')

    log_level = ArConfig.get('logging', 'log_level')
    log = init_log(log_mode, log_file, log_level, 'argo.upload_metric')

    # Inform the user in wether argo runs locally or distributed
    if ar_mode == 'local':
        log.info("ARGO compute engine runs in LOCAL mode")
        log.info("metric data will be staged for computations locally")
    else:
        log.info("ARGO compute engine runs in CLUSTER mode")
        log.info("metric data will be uploaded to HDFS")

    # call prefilter if necessary for specified tenant
    if ArConfig.has_option('jobs', args.tenant + '_prefilter'):
        prefilter_exec = ArConfig.get('jobs', args.tenant + '_prefilter')
        cmd_pref = [os.path.join(arsync_exec, prefilter_exec), '-d', args.date]

        log.info("Calling %s for date: %s", os.path.join(arsync_exec, prefilter_exec), args.date)

        run_cmd(cmd_pref, log)

        fn_prefilter = "prefilter_" + date_under + ".avro"
        local_prefilter = os.path.join(arsync_lib, args.tenant, fn_prefilter)

        log.info("Check if produced %s exists: %s",
                 local_prefilter, os.path.exists(local_prefilter))

    # if prefilter not necessary, copy the orignal data file as a prefiltered result
    # so as to be picked up and transfered to hdfs
    else:
        fn_mdata = 'argo-consumer_log_' + args.date + '.avro'
        fn_prefilter = "prefilter_" + date_under + ".avro"
        local_mdata = os.path.join(consumers_root,'argo-'+args.tenant.lower()+'-consumer',fn_mdata)
        local_prefilter = os.path.join(arsync_lib, args.tenant, fn_prefilter)
        cmd_copy = ['cp', local_mdata , local_prefilter ]
        run_cmd(cmd_copy,log)


    if ar_mode == 'cluster':
        # compose hdfs destination
        # hdfs path = ./tenant/mdata/...
        hdfs_path = "./" + args.tenant + "/mdata/"
    else:
        # compose local temporary destination
        hdfs_path = "/tmp/" + args.tenant + "/mdata/"

    # Command to establish tentant's metric data hdfs folder
    cmd_hdfs_mkdir = ['hadoop', 'fs', '-mkdir', '-p', hdfs_path]

    # Put file to hdfs destination
    cmd_hdfs = ['hadoop', 'fs', '-put', '-f', local_prefilter, hdfs_path]

    # Command to clear prefilter data after hdfs transfer
    cmd_clean = ['rm', '-f', local_prefilter]

    log.info("Establish if not present hdfs metric data directory")
    run_cmd(cmd_hdfs_mkdir, log)

    log.info("Transfer files to hdfs")
    run_cmd(cmd_hdfs, log)

    if prefilter_clean == "true":
        log.info("System configured to clean prefilter data after transfer")
        run_cmd(cmd_clean, log)

    log.info("Metric Data of tenant %s for date %s uploaded successfully to hdfs",
             args.tenant, args.date)

if __name__ == "__main__":

    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(description="Uploading metric data to hdfs")
    arg_parser.add_argument(
        "-d", "--date", help="date", dest="date", metavar="DATE", required="TRUE")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner ", dest="tenant", metavar="STRING", required="TRUE")

    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
