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

	ArConfig = SafeConfigParser()
	ArConfig.read(fn_ar_cfg)
	
	mongo_host = ArConfig.get('default','mongo_host')
	mongo_port = ArConfig.get('default','mongo_port')
	client = MongoClient(str(mongo_host), int(mongo_port))
	db = client.AR
	date_int = int(args.date.replace("-",""))
	db.sites.remove({"dt": date})
	db.sfreports.remove({"dt":date})

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="clean status detail data for a day")
	arg_parser.add_argument("-d","--date",help="date", dest="date", metavar="DATE", required="TRUE")


	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
