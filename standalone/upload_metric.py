#!/usr/bin/env python

# arg parsing related imports
import os, sys, logging
from subprocess import call
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser

def prepare_logger(log_file,log_level,log_name):
	# Instantiate logger with proper name
	logger = logging.getLogger(log_name)
	logger.setLevel(logging.DEBUG)
	# Define two handlers (file / console)
	file_log = logging.FileHandler(log_file)
	cmd_log = logging.StreamHandler()
	# The log level for console is always up to info
	cmd_log.setLevel(logging.INFO)
	# The log level for file is parametized
	if log_level == 'DEBUG' :
		file_log.setLevel(logging.DEBUG)
	elif log_level == 'INFO':
		file_log.setLevel(logging.INFO)
	elif log_level == 'WARNING':
		file_log.setLevel(logging.WARNING)
	elif log_level == 'ERROR':
		file_log.setLevel(logging.ERROR)
	elif log_level == 'CRITICAL':
		file_log.setLevel(logging.CRITICAL)
	# Define the format for the file log formatting
	file_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
	file_log.setFormatter(file_format)
	# Add handlers 
	logger.addHandler(file_log)
	logger.addHandler(cmd_log)

	return logger


def main(args=None):

	
	# default config 
	fn_ar_cfg = "/etc/ar-compute-engine.conf"
	arsync_exec = "/usr/libexec/ar-sync/"
	arsync_lib = "/var/lib/ar-sync/"

	date_under = args.date.replace("-","_")

	ArConfig = SafeConfigParser()
	ArConfig.read(fn_ar_cfg)

	# Initialize logging
	log_file = ArConfig.get('logging','log_file')
	log_level = ArConfig.get('logging','log_level')
	logger = prepare_logger(log_file,log_level,'[upload_metric.py]')

	prefilter_clean = ArConfig.get('default','prefilter_clean')

	#call prefilter
	cmd_pref = [os.path.join(arsync_exec,'prefilter-avro'),'-d',args.date]
	
	logger.info("Calling prefilter-avro for date:%s",args.date) 
	call(cmd_pref)

	#transfer prefilter to hdfs
	hdfs_path = "./"+args.tenant+"/mdata/"
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

	logger.info("Establish if not present hdfs metric data directory")
	call(cmd_hdfs)

	if prefilter_clean == "true":

		logger.info("System configured to clean prefilter data after transfer")
		call(cmd_clean)

	logger.info("Metric Data of tenant %s for date %s uploaded successfully to hdfs",args.tenant,args.date)
	

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="Uploading metric data to hdfs")
	arg_parser.add_argument("-d","--date",help="date", dest="date", metavar="DATE", required="TRUE")
	arg_parser.add_argument("-t","--tenant",help="tenant owner ", dest="tenant", metavar= "STRING", required="TRUE")

	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
