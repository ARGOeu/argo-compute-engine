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
	sdl_exec = "/usr/libexec/ar-compute/standalone/"
	

	ArConfig = SafeConfigParser()
	ArConfig.read(fn_ar_cfg)

	# Initialize logging
	log_mode = ArConfig.get('logging', 'log_mode')
	log_file='none'

	if log_mode=='file':
		log_file = ArConfig.get('logging', 'log_file')	
    
	log_level = ArConfig.get('logging', 'log_level')
	log = init_log(log_mode, log_file, log_level, '[job_cycle.py]')

	tenant = ArConfig.get("jobs","tenant")
	job_set = ArConfig.get("jobs","job_set")
	job_set = job_set.split(',')

	# Notify that he job cycle has begun
	log.info("Job Cycle: started for tenant:%s and date: %s",tenant,args.date)

	# Command to upload the prefilter data
	cmd_upload_metric = [os.path.join(sdl_exec,"upload_metric.py"),'-d',args.date,'-t',tenant]

	log.info("Job Cycle: Upload metric data to hdfs")
	run_cmd(cmd_upload_metric,log)

	# Command to submit job status detail
	cmd_job_status = [os.path.join(sdl_exec,"job_status_detail.py"),'-d',args.date,'-t',tenant]

	log.info("Job Cycle: Run status detail job")
	run_cmd(cmd_job_status,log)

	log.info("Job Cycle: Iterate over a/r jobs and submit them")
	# For each job genereate ar
	for item in job_set:

		cmd_job_ar = [os.path.join(sdl_exec,"job_ar.py"),'-d',args.date,'-t',tenant,'-j',item]
		run_cmd(cmd_job_ar,log)

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="Cycling daily jobs for a tenant")
	arg_parser.add_argument("-d","--date",help="date", dest="date", metavar="DATE", required="TRUE")
	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
