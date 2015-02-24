#!/usr/bin/env python

# arg parsing related imports
import os, sys
from subprocess import check_call
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

	tenant = ArConfig.get("jobs","tenant")
	job_set = ArConfig.get("jobs","job_set")
	job_set = job_set.split(',')



	#First upload the prefilter data
	cmd_upload_metric = [os.path.join(sdl_exec,"upload_metric.py"),'-d',args.date,'-t',tenant]

	try:
		check_call(cmd_upload_metric)
	except Exception, err:
		sys.stderr.write('Could not upload metric data to hdfs \n')
		return 1

	#Upload the first job sync data for status_detail generation
	cmd_upload_sync = [os.path.join(sdl_exec,"upload_sync.py"),'-d',args.date,'-t',tenant,'-j',job_set[0]]

	try:
		check_call(cmd_upload_sync)
	except Exception, err:
		sys.stderr.write('Could not upload sync data to hdfs \n')
		return 1


	#clean statuses from mongo
	cmd_mongo_clean_status = [os.path.join(sdl_exec,"mongo_clean_status.py"),'-d',args.date]
	try:
		check_call(cmd_mongo_clean_status)
	except Exception, err:
		sys.stderr.write('Could not clean mongo status data \n')
		return 1


	#Generate status details 
	cmd_job_status = [os.path.join(sdl_exec,"job-status.py"),'-d',args.date,'-t',tenant,'-j',job_set[0]]
	try:
		check_call(cmd_job_status)
	except Exception, err:
		sys.stderr.write('Could not run status detail job \n')
		return 1

	#clean ar results
	cmd_mongo_clean_ar = [os.path.join(sdl_exec,"mongo_clean_ar.py"),'-d',args.date]
	try:
		check_call(cmd_mongo_clean_ar)
	except Exception, err:
		sys.stderr.write('Could not clean mongo ar data \n')
		return 1

	#For each job genereate ar
	for item in job_set:
		cmd_upload_sync = [os.path.join(sdl_exec,"upload_sync.py"),'-d',args.date,'-t',tenant,'-j',item]

		try:
			check_call(cmd_mongo_clean_ar)
		except Exception, err:
			sys.stderr.write('Could not upload sync data to hdfs \n')
			return 1

		cmd_job_ar = [os.path.join(sdl_exec,"job-ar.py"),'-d',args.date,'-t',tenant,'-j',item]

		try:
			check_call(cmd_job_ar)
		except Exception, err:
			sys.stderr.write('Could not run ar job \n')
			return 1

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="Cycling daily jobs for a tenant")
	arg_parser.add_argument("-d","--date",help="date", dest="date", metavar="DATE", required="TRUE")
	

	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
