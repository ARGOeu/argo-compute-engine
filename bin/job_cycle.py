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
    arcomp_conf = "/etc/ar-compute/"
    sdl_exec = "/usr/libexec/ar-compute/bin/"

    ArConfig = SafeConfigParser()
    ArConfig.read(fn_ar_cfg)

    # Initialize logging
    log_mode = ArConfig.get('logging', 'log_mode')
    log_file = None

    if log_mode == 'file':
        log_file = ArConfig.get('logging', 'log_file')

    # Set hadoop root logger settings
    os.environ["HADOOP_ROOT_LOGGER"] = ArConfig.get(
        'logging', 'hadoop_log_root')

    log_level = ArConfig.get('logging', 'log_level')
    log = init_log(log_mode, log_file, log_level, 'argo.job_cycle')

    # Get the available tenants from config file
    tenant_list = ArConfig.get("jobs", "tenants")
    tenant_list = tenant_list.split(',')

    if args.tenant is not None:
        tenant_list = [args.tenant]

    # For each available tenant prepare and execute all tenant's jobs
    for tenant in tenant_list:

        # Get specific tenant's job set
        job_set = ArConfig.get("jobs", tenant + '_jobs')
        job_set = job_set.split(',')

        # Notify that he job cycle has begun
        log.info(
            "Job Cycle: started for tenant:%s and date: %s", tenant, args.date)

        # Command to upload the prefilter data
        cmd_upload_metric = [
            os.path.join(sdl_exec, "upload_metric.py"), '-d', args.date, '-t', tenant]

        log.info("Job Cycle: Upload metric data to hdfs")
        run_cmd(cmd_upload_metric, log)

        log.info("Job Cycle: Iterate over jobs and submit them")

        # For each job genereate ar
        for job in job_set:
            log.info("Job Cycle: tenant %s has job named %s", tenant, job)
            cmd_job_ar = [os.path.join(sdl_exec, "job_ar.py"), '-d', args.date, '-t', tenant, '-j', job]
            run_cmd(cmd_job_ar, log)
            # Command to submit job status detail
            cmd_job_status = [os.path.join(sdl_exec, "job_status_detail.py"), '-d', args.date, '-t', tenant, '-j', job]
            run_cmd(cmd_job_status, log)
            

if __name__ == "__main__":

    # Feed Argument parser with the description of the 3 arguments we need
    # (input_file,output_file,schema_file)
    arg_parser = ArgumentParser(
        description="Cycling daily jobs for all available tenants")
    arg_parser.add_argument(
        "-d", "--date", help="date", dest="date", metavar="DATE", required="TRUE")
    arg_parser.add_argument(
        "-t", "--tenant", help="tenant", dest="tenant", metavar="STRING")
    # Parse the command line arguments accordingly and introduce them to
    # main...
    sys.exit(main(arg_parser.parse_args()))
