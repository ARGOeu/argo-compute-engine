#!/usr/bin/env python

# arg parsing related imports
import os, sys
from subprocess import call
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser



def main(args=None):

	
	# default config 
	fn_ar_cfg = "/etc/ar-compute-engine.conf"
	arsync_exec = "/usr/libexec/ar-sync/"
	arsync_lib = "/var/lib/ar-sync/"

	date_under = args.date.replace("-","_")

	ArConfig = SafeConfigParser()
	ArConfig.read(fn_ar_cfg)

	# Get mode from config file
	ar_mode = ArConfig.get('default','mode')

	prefilter_clean = ArConfig.get('default','prefilter_clean')

	# Initialize logging
    log_mode = ArConfig.get('logging', 'log_mode')
    log_file='none'

    if log_mode=='file':
		log_file = ArConfig.get('logging', 'log_file')	
    
    log_level = ArConfig.get('logging', 'log_level')
    logger = init_logger(log_mode, log_file, log_level, '[upload_metric.py]')

	#call prefilter
	cmd_pref = [os.path.join(arsync_exec,'prefilter-avro'),'-d',args.date]
	
	logger.info("Calling prefilter-avro for date:%s",args.date)

	call(cmd_pref)

	if ar_mode == 'cluster':
		# compose hdfs destination
		# hdfs path = ./tenant/mdata/...
		hdfs_path = "./"+args.tenant+"/mdata/"
	else:
		# compose local temporary destination
		hdfs_path = "/tmp/"+args.tenant+"/mdata/"

	fn_prefilter = "prefilter_"+date_under+".avro"
	local_prefilter = os.path.join(arsync_lib,fn_prefilter)

	logger.info("Check if produced %s exists: %s",local_prefilter,os.path.exists(local_prefilter))

	# Command to establish tentant's metric data hdfs folder 
	cmd_hdfs_mkdir = ['hadoop','fs','-mkdir','-p',hdfs_path]
	
	# Put file to hdfs destination
	cmd_hdfs = ['hadoop','fs','-put','-f',local_prefilter,hdfs_path]

	# Command to clear prefilter data after hdfs transfer
	cmd_clean = ['rm',local_prefilter]


	logger.info("Establish if not present hdfs metric data directory")
	call(cmd_hdfs_mkdir)

	logger.info("Transfer files to hdfs")
	call(cmd_hdfs)

	if prefilter_clean == "true":
		logger.info("System configured to clean prefilter data after transfer")
		call(cmd_clean)


	logger.info("Metric Data of tenant %s for date %s uploaded successfully to hdfs",args.tenant , args.date)

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="Uploading metric data to hdfs")
	arg_parser.add_argument("-d","--date",help="date", dest="date", metavar="DATE", required="TRUE")
	arg_parser.add_argument("-t","--tenant",help="tenant owner ", dest="tenant", metavar= "STRING", required="TRUE")

	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
