#!/usr/bin/env python

import os
import sys
import json
import glob
import tarfile
from argolog import init_log
from argorun import run_cmd
from datetime import datetime, timedelta
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser


def main(args=None):

    # default config
    fn_ar_cfg = "/etc/ar-compute-engine.conf"
    arsync_exec = "/usr/libexec/ar-sync/"
    arsync_lib = "/var/lib/ar-sync/"
    arcomp_conf = "/etc/ar-compute/"
    arcomp_exec = "/usr/libexec/ar-compute/"
    stdl_exec = "/usr/libexec/ar-compute/standalone"
    pig_script_path = "/ufsr/libexec/ar-compute/pig/"

    ArConfig = SafeConfigParser()
    ArConfig.read(fn_ar_cfg)

    # Parse date argument
    actual_date = datetime.strptime(args.date, '%Y-%m-%d')
    # Set day on the first of the month
    actual_date = actual_date.replace(day=1)
    # First day of the month minus one day gets us back one month ago (or even
    # year ago)
    month_ago = actual_date - timedelta(days=1)

    fn_sync_tar = 'sync_backup_' + args.tenant + '_' + \
        str(month_ago.strftime("%B")) + '_' + str(month_ago.year) + '.tar'

    local_tar = os.path.join('/tmp', fn_sync_tar)

    # Check if tar file already exists from a previous backup try and remove it
    if os.path.exists(local_tar):
        os.remove(local_tar)

    sync_tar = tarfile.open(local_tar, mode='w')

    # Grab all available jobs in the system
    job_set = ArConfig.get("jobs", "job_set")
    job_set = job_set.split(',')

    # Create query strings to list appropriate files
    query_down = '*_' + \
        month_ago.strftime("%Y") + "_" + month_ago.strftime("%m") + '_*.avro'
    query_dash = '*_' + \
        month_ago.strftime("%Y") + "-" + month_ago.strftime("%m") + '-*.avro'

    # Downtimes are special because they always reside in the ar-sync root
    # (might change in the future)
    downtime_list = glob.glob(os.path.join(arsync_lib, query_dash))

    # Add downtimes to the tar file

    log.info("Adding downtime files for %s of %s",
             month_ago.strftime("%B"), month_ago.year)
    for f in downtime_list:
        tar_path = args.tenant + '/' + os.path.basename(f)
        sync_tar.add(f, tar_path)

    # Iterate over job folders
    for item in job_set:
        jobsync_list = glob.glob(
            os.path.join(arsync_lib, args.tenant, item, query_down))

        log.info("adding sync files for %s of %s for Job: %s",
                 month_ago.strftime("%B"), month_ago.year, item)

        for f in jobsync_list:
            tar_path = args.tenant + '/' + item + '/' + os.path.basename(f)
            sync_tar.add(f, tar_path)

    sync_tar.close()

    # Create HDFS backup path
    hdfs_dest = args.tenant + '/backup/sync/'
    cmd_establish_hdfs = ['hadoop', 'fs', '-mkdir', '-p', hdfs_dest]

    log.info("Establish hdfs backup directory: %s", hdfs_dest)
    run_cmd(cmd_establish_hdfs, log)

    # Transfer tar archive to hdfs
    cmd_hdfs_put = ['hadoop', 'fs', '-put', '-f', local_tar, hdfs_dest]

    log.info("Transfer backup  from local:%s to hdfs:%s", local_tar, hdfs_dest)
    run_cmd(cmd_hdfs_put, log)

    # Clean up temporary tar
    log.info("Cleanup tmp data")
    if os.path.exists(local_tar):
        os.remove(local_tar)

    log.info("Backup Completed to hdfs")


if __name__ == "__main__":
    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(description="Initiate an a/r computation job")
    arg_parser.add_argument(
        "-d", "--date", help="date month will be targeted", dest="date", metavar="DATE", required="TRUE")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner ", dest="tenant", metavar="STRING", required="TRUE")
    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
