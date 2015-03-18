#!/usr/bin/env python

# arg parsing related imports
import os
import sys
from argologger import init_logger
from subprocess import call
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser
from pymongo import MongoClient



def main(args=None):

	# default config 
	fn_ar_cfg = "/etc/ar-compute-engine.conf"
	arcomp_conf = "/etc/ar-compute/"
	sdl_exec = "/usr/libexec/ar-compute/standalone/"
	

	ArConfig = SafeConfigParser()
	ArConfig.read(fn_ar_cfg)

	# Initialize logging
	log_mode = ArConfig.get('logging','log_mode')
	log_file = ArConfig.get('logging','log_file')
	log_level = ArConfig.get('logging','log_level')
	logger = init_logger(log_mode,log_file,log_level,'[job_cycle.py]')

	tenant = ArConfig.get("jobs","tenant")
	job_set = ArConfig.get("jobs","job_set")
	job_set = job_set.split(',')


	#Command to upload the prefilter data
	cmd_upload_metric = [os.path.join(sdl_exec,"upload_metric.py"),'-d',args.date,'-t',tenant]

	logger.info("UPLOAD METRIC DATA TO HDFS")
	call(cmd_upload_metric)

	#Command to submit job status detail

	cmd_job_status = [os.path.join(sdl_exec,"job_status_detail.py"),'-d',args.date,'-t',tenant]

	logger.info("CALCULATE STATUS DETAIL")
	call(cmd_job_status)

	logger.info("Iterate over a/r jobs and submit them")
	#For each job genereate ar
	for item in job_set:

		cmd_job_ar = [os.path.join(sdl_exec,"job_ar.py"),'-d',args.date,'-t',tenant,'-j',item]
		call(cmd_job_ar)

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="Cycling daily jobs for a tenant")
	arg_parser.add_argument("-d","--date",help="date", dest="date", metavar="DATE", required="TRUE")
	

	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
