#!/usr/bin/env python

# arg parsing related imports
import os
import sys
from argolog import init_log
from argorun import run_cmd
from datetime import datetime, timedelta
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser


def getSyncFile(dt, prefix, postfix, splitstr, log):
    days_back = 0
    while True:
        target_dt = dt - timedelta(days=days_back)
        date_split = target_dt.strftime(
            '%Y' + splitstr + '%m' + splitstr + '%d')
        file_path = prefix + date_split + postfix

        log.info("Check if %s exists...", file_path)

        if (os.path.exists(file_path)):
            log.info("True")
            return file_path
        else:
            days_back = days_back + 1
            log.warning("False, try %s days back", str(days_back))

        if days_back > 3:
            log.critical("Error! Too many days without a file...")
            sys.exit(1)


def main(args=None):

    # Default core paths
    fn_ar_cfg = "/etc/ar-compute-engine.conf"
    arcomp_conf = "/etc/ar-compute/"
    argo_exec = "/usr/libexec/ar-compute/bin/"

    actual_date = datetime.strptime(args.date, '%Y-%m-%d')

    # Create a second date used by the file formats
    date_under = args.date.replace("-", "_")

    # Initiate config file parser to read global ar-compute-engine.conf
    ArConfig = SafeConfigParser()
    ArConfig.read(fn_ar_cfg)

    # Get sync exec and path
    arsync_exec = ArConfig.get('connectors', 'sync_exec')
    arsync_lib = ArConfig.get('connectors', 'sync_path')


    # Initialize logging
    log_mode = ArConfig.get('logging', 'log_mode')
    log_file = None

    if log_mode == 'file':
        log_file = ArConfig.get('logging', 'log_file')

    log_level = ArConfig.get('logging', 'log_level')
    log = init_log(log_mode, log_file, log_level, 'argo.upload_sync')

    # Get mode from config file
    ar_mode = ArConfig.get('default', 'mode')

    # Inform the user in wether argo runs locally or distributed
    if ar_mode == 'local':
        log.info("ARGO compute engine runs in LOCAL mode")
        log.info("sync data will be staged for computations locally")
    else:
        log.info("ARGO compute engine runs in CLUSTER mode")
        log.info("sync data will be  uploaded to HDFS")

    # Compose needed sync filenames using the correct prefixes, dates and file
    # extensions (avro/json)

    fn_ops = args.tenant + '_ops.json'
    fn_aps = args.tenant + '_' + args.job + '_ap.json'
    fn_cfg = args.tenant + '_' + args.job + '_cfg.json'
    fn_rec = "recomputations_" + args.tenant + "_" + date_under + ".json"

    if ar_mode == 'cluster':
        # compose hdfs temporary destination
        # hdfs dest = ./scratch/sync/tenant/job/date/...
        # sync files are not meant to be kept in hdfs (unless archived in
        # batches)
        hdfs_dest = './scratch/sync/' + args.tenant + \
            '/' + args.job + '/' + date_under + '/'
    else:
        # compose local temporary destination
        hdfs_dest = '/tmp/scratch/sync/' + args.tenant + \
            '/' + args.job + '/' + date_under + '/'

    # Compose the local ar-sync files job folder
    # arsync job = /path/to/synced_stuff/tenant/job/...
    arsync_job = arsync_lib + '/' + args.tenant + '/' + args.job + '/'

    # Call downtimes latest info
    cmd_call_downtimes = [
        os.path.join(arsync_exec, 'downtimes-gocdb-connector.py'), '-d', args.date]
    log.info("Calling downtime sync connector to give us latest downtime info")

    run_cmd(cmd_call_downtimes, log)

    # Call script to retrieve a json file of recomputations for the specific date/tenant from mongodb
    cmd_mongo_recomputations = [os.path.join(argo_exec, 'mongo_recompute.py'), '-d', args.date, '-t', args.tenant, '-j', args.job]
    log.info("Retrieving relevant recomputation requests...")
    run_cmd(cmd_mongo_recomputations, log)

    # Compose the local paths for files (paths+filenames)
    local_egroups = getSyncFile(
        actual_date, os.path.join(arsync_job, "group_endpoints_"), '.avro', '_', log)
    local_ggroups = getSyncFile(
        actual_date, os.path.join(arsync_job, "group_groups_"), '.avro', '_', log)
    local_weights = getSyncFile(
        actual_date, os.path.join(arsync_job, "weights_"), '.avro', '_', log)
    local_mps = getSyncFile(
        actual_date, os.path.join(arsync_job, "poem_sync_"), '.avro', '_', log)
    local_downtimes = getSyncFile(
        actual_date, os.path.join(arsync_job, "downtimes_"), '.avro', '_', log)

    local_aps = os.path.join(arcomp_conf, fn_aps)
    local_ops = os.path.join(arcomp_conf, fn_ops)
    local_cfg = os.path.join(arcomp_conf, fn_cfg)
    local_rec = os.path.join(arsync_lib, fn_rec)

    # Check filenames if exist
    log.info("Check if %s exists: %s", local_aps, os.path.exists(local_aps))
    log.info("Check if %s exists: %s", local_ops, os.path.exists(local_ops))
    log.info("Check if %s exists: %s", local_cfg, os.path.exists(local_cfg))
    log.info("Check if %s exists: %s", local_rec, os.path.exists(local_rec))

    # Remove scratch sync directory in hdfs (cause we don't keep unarchived
    # sync files)
    cmd_clearHdfs = ['hadoop', 'fs', '-rm', '-r', '-f', hdfs_dest]
    # Establish new scratch sync directory in hdfs for this job
    cmd_estHdfs = ['hadoop', 'fs', '-mkdir', '-p', hdfs_dest]
    # Transfer endpoint groups topo from local to hdfs
    cmd_putEgroups = ['hadoop', 'fs', '-put', '-f',
                      local_egroups, hdfs_dest + 'group_endpoints.avro']
    # Transfer group of groups topo from local to hdfs
    cmd_putGgroups = ['hadoop', 'fs', '-put', '-f',
                      local_ggroups, hdfs_dest + 'group_groups.avro']
    # Transfer weight factors from local to hdfs
    cmd_putWeights = ['hadoop', 'fs', '-put', '-f',
                      local_weights, hdfs_dest + 'weights.avro']
    # Transfer metric profile from local to hdfs
    cmd_putMps = ['hadoop', 'fs', '-put', '-f',
                  local_mps, hdfs_dest + 'poem_sync.avro']
    # Transfer downtime info from local to hdfs
    cmd_putDowntimes = ['hadoop', 'fs', '-put', '-f',
                        local_downtimes, hdfs_dest + 'downtimes.avro']

    # Transfer availability profile from local to hdfs
    cmd_putAps = ['hadoop', 'fs', '-put', '-f', local_aps, hdfs_dest]
    # Transfer operations from local to hdfs
    cmd_putOps = ['hadoop', 'fs', '-put', '-f', local_ops, hdfs_dest]
    # Transfer job configuration file from local to hdfs
    cmd_putCfg = ['hadoop', 'fs', '-put', '-f', local_cfg, hdfs_dest]
    # Transfer recalculations requests (if any) from local to hdfs
    cmd_putRec = ['hadoop', 'fs', '-put', '-f', local_rec, hdfs_dest]

    # try:
    log.info("Remove old scratch sync folder: %s", hdfs_dest)
    run_cmd(cmd_clearHdfs, log)
    log.info("Establish new scratch sync folder %s", hdfs_dest)
    run_cmd(cmd_estHdfs, log)
    log.info("Transfer metric profile")
    run_cmd(cmd_putMps, log)
    log.info("Transfer endpoint group topology")
    run_cmd(cmd_putEgroups, log)
    log.info("Transfer group of group topology")
    run_cmd(cmd_putGgroups, log)
    log.info("Transfer weight factors")
    run_cmd(cmd_putWeights, log)
    log.info("Transfer downtimes")
    run_cmd(cmd_putDowntimes, log)
    log.info("Transfer availability profile")
    run_cmd(cmd_putAps, log)
    log.info("Transfer operations file")
    run_cmd(cmd_putOps, log)
    log.info("Transfer job configuration")
    run_cmd(cmd_putCfg, log)
    log.info("Transfer recalculation requests")
    run_cmd(cmd_putRec, log)

    # Clear local temporary recomputation file
    os.remove(local_rec)

    log.info("Sync Data of tenant %s for job %s for date %s uploaded successfully to hdfs",
             args.tenant, args.job, args.date)



if __name__ == "__main__":

    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(description="Uploading sync data to hdfs")
    arg_parser.add_argument(
        "-d", "--date", help="date", dest="date", metavar="DATE", required="TRUE")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner", dest="tenant", metavar="STRING", required="TRUE")
    arg_parser.add_argument(
        "-j", "--job", help="job name", dest="job", metavar="STRING", required="TRUE")

    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
