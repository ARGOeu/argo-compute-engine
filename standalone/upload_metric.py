#!/usr/bin/env python

# arg parsing related imports
import os, sys
from subprocess import check_call
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


	#call prefilter
	cmd_pref = [os.path.join(arsync_exec,'prefilter-avro'),'-d',args.date]
	

	try:
		check_call(cmd_pref)
	except Exception, err:

		sys.stderr.write('Could not run prefilter-avro properly \n')
		return 1

	#transfer prefilter to hdfs
	hdfs_path = "./"+args.tenant+"/mdata/"
	fn_prefilter = "prefilter_"+date_under+".avro"

	

	cmd_hdfs = ['hadoop','fs','-put','-f',os.path.join(arsync_lib,fn_prefilter),hdfs_path]

	try:
		check_call(cmd_hdfs)
	except Exception, err:

		sys.stderr.write('Could not upload metric data to hdfs \n')
		return 1

	print "Metric Data of tenant %s for date %s uploaded successfully to hdfs" % (args.tenant , args.date)

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="Uploading metric data to hdfs")
	arg_parser.add_argument("-d","--date",help="date", dest="date", metavar="DATE", required="TRUE")
	arg_parser.add_argument("-t","--tenant",help="tenant owner ", dest="tenant", metavar= "STRING", required="TRUE")

	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
