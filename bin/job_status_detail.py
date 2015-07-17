#!/usr/bin/env python

# arg parsing related imports
import os
import sys
import json
import utils
from argolog import init_log
from argorun import run_cmd
from datetime import datetime, timedelta
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser


def main(args=None):

    # default paths
    fn_ar_cfg = "/etc/ar-compute-engine.conf"
    arcomp_conf = "/etc/ar-compute/"
    arcomp_exec = "/usr/libexec/ar-compute/"
    stdl_exec = "/usr/libexec/ar-compute/bin"
    pig_script_path = "/usr/libexec/ar-compute/pig/"

    one_day_ago = utils.get_actual_date(args.date) - timedelta(days=1)
    prev_date = utils.get_date_str(one_day_ago)
    prev_date_under = utils.get_date_under(prev_date)
    date_under = utils.get_date_under(args.date)

    # Init configuration
    cfg = ArgoConfiguration(fn_ar_cfg)
    cfg.load_tenant_db_conf(self, os.path.join(arcomp_conf, args.tenant + "_db_conf.json"))
    # Init logging
    log = init_log(cfg.log_mode, cfg.log_file, cfg.log_level, 'argo.job_status_detail')

    job_set = cfg.jobs[args.tenant]

    # Inform the user in wether argo runs locally or distributed
    if cfg.mode == 'local':
        log.info("ARGO compute engine runs in LOCAL mode")
        log.info("computation job will be run locally")
    else:
        log.info("ARGO compute engine runs in CLUSTER mode")
        log.info("computation job will be submitted to the hadoop cluster")

    # Proposed hdfs pathways
    hdfs_mdata_path = './' + args.tenant + "/mdata/"
    hdfs_sync_path = './scratch/sync/' + args.tenant + \
        "/" + job_set[0] + "/" + date_under + "/"

    # Proposed local pathways
    local_mdata_path = '/tmp/' + args.tenant + "/mdata/"
    local_sync_path = '/tmp/scratch/sync/' + args.tenant + \
        '/' + job_set[0] + '/' + date_under + '/'
    local_cfg_path = arcomp_conf

    if cfg.mode == 'cluster':
        mode = 'cache'
        mdata_path = hdfs_mdata_path
        sync_path = hdfs_sync_path
        cfg_path = hdfs_sync_path

    else:
        mode = 'local'
        mdata_path = local_mdata_path
        sync_path = local_sync_path
        cfg_path = local_cfg_path

    # dictionary with necessary pig parameters
    pig_params = {}

    pig_params['mdata'] = mdata_path + 'prefilter_' + date_under + '.avro'
    pig_params['p_mdata'] = mdata_path + \
        'prefilter_' + prev_date_under + '.avro'
    pig_params['egs'] = sync_path + 'group_endpoints.avro'
    pig_params['ggs'] = sync_path + 'group_groups.avro'
    pig_params['mps'] = sync_path + 'poem_sync.avro'
    pig_params['cfg'] = cfg_path + args.tenant + '_' + job_set[0] + '_cfg.json'
    pig_params['aps'] = cfg_path + args.tenant + '_' + job_set[0] + '_ap.json'
    pig_params['dt'] = args.date
    pig_params['mode'] = mode
    pig_params['n_eg'] = ArConfig.get('datastore_mapping', 'n_eg')
    pig_params['n_gg'] = ArConfig.get('datastore_mapping', 'n_gg')
    pig_params['n_alt'] = ArConfig.get('datastore_mapping', 'n_alt')
    pig_params['n_altf'] = ArConfig.get('datastore_mapping', 'n_altf')
    pig_params['sd_map'] = ArConfig.get('datastore_mapping', 'sd_map')
    pig_params['flt'] = '0'
    pig_params['mongo_status_detail'] = cfg.get_mongo_uri('status_metric')

    cmd_pig = []

    # Append pig command
    cmd_pig.append('pig')

    # Append Pig local execution mode flag
    if cfg.mode == "local":
        cmd_pig.append('-x')
        cmd_pig.append('local')

    # Append Pig Parameters
    for item in pig_params:
        cmd_pig.append('-param')
        cmd_pig.append(item + '=' + pig_params[item])

    # Append Pig Executionable Script
    cmd_pig.append('-f')
    cmd_pig.append(pig_script_path + 'compute-status.pig')

    # Command to clean a/r data from mongo
    cmd_clean_mongo_status = [
        os.path.join(stdl_exec, "mongo_clean_status.py"), '-d', args.date]

    # Command to upload sync data to hdfs
    cmd_upload_sync = [os.path.join(
        stdl_exec, "upload_sync.py"), '-d', args.date, '-t', args.tenant, '-j', job_set[0]]

    # Command to clean hdfs data
    cmd_clean_sync = ['hadoop', 'fs', '-rm', '-r', '-f', hdfs_sync_path]

    # Upload data to hdfs
    log.info("Uploading sync data to hdfs...")
    run_cmd(cmd_upload_sync, log)

    # Clean data from mongo
    log.info("Cleaning data from mongodb")
    run_cmd(cmd_clean_mongo_status, log)

    # Call pig
    log.info("Submitting pig compute status detail job...")
    run_cmd(cmd_pig, log)

    # Cleaning hdfs sync data
    if cfg.sync_clean == "true":
        log.info("System configured to clean sync hdfs data after job")
        run_cmd(cmd_clean_sync, log)

    log.info("Execution of status job for tenant %s for date %s completed!",
             args.tenant, args.date)

if __name__ == "__main__":

    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(description="Initiate a status detail job")
    arg_parser.add_argument(
        "-d", "--date", help="date", dest="date", metavar="DATE", required="TRUE")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant owner ", dest="tenant", metavar="STRING", required="TRUE")
    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
