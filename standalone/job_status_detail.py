#!/usr/bin/env python

# arg parsing related imports
import os, sys
from datetime import datetime, timedelta
from subprocess import check_call
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser

def main(args=None):

	# default config 
	fn_ar_cfg = "/etc/ar-compute-engine.conf"
	arsync_exec = "/usr/libexec/ar-sync/"
	arsync_lib = "/var/lib/ar-sync/"
	arcomp_conf = "/etc/ar-compute/"
	arcomp_exec = "/usr/libexec/ar-compute/"
	pig_script_path = "/usr/libexec/ar-compute/pig/"

	actual_date = datetime.strptime(args.date,'%Y-%m-%d')
	one_day_ago = actual_date - timedelta(days=1)
	prev_date = one_day_ago.strftime('%Y-%m-%d')
	prev_date_under = prev_date.replace("-","_")

	date_under = args.date.replace("-","_")

	ArConfig = SafeConfigParser()
	ArConfig.read(fn_ar_cfg)

	mongo_host = ArConfig.get('default','mongo_host')
	mongo_port = ArConfig.get('default','mongo_port')
	mongo_dest = ArConfig.get('datastore_mapping','sdetail_dest')
	ar_mode = ArConfig.get('default','mode')
	

	# Proposed hdfs pathways
	hdfs_mdata_path = './' + args.tenant + "/mdata/"
	hdfs_sync_path = './' + args.tenant + "/sync/" + args.job + "/" 

	# Proposed local pathways
	local_mdata_path = arsync_lib
	local_sync_path = arsync_lib + args.tenant + "/" + args.job + "/"
	local_cfg_path = arcomp_conf 

	if ar_mode == 'cluster':
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
	pig_params={}

	pig_params['mdata'] = mdata_path + 'prefilter_' + date_under + '.avro';
	pig_params['p_mdata'] = mdata_path + 'prefilter_' + prev_date_under + '.avro';
	pig_params['egs'] = sync_path + "group_endpoints_" + date_under + '.avro';
	pig_params['ggs'] = sync_path + "group_groups_" + date_under + '.avro';
	pig_params['mps'] = sync_path + "poem_sync_" + date_under + '.avro';
	pig_params['cfg'] = cfg_path + args.tenant + "_" + args.job + '_cfg.json';
	pig_params['dt'] = args.date 
	pig_params['mode'] = mode
	pig_params['n_eg'] = ArConfig.get('datastore_mapping','n_eg')
	pig_params['n_gg'] = ArConfig.get('datastore_mapping','n_gg')
	pig_params['n_alt'] = ArConfig.get('datastore_mapping','n_alt')
	pig_params['n_altf'] = ArConfig.get('datastore_mapping','n_altf') 
	pig_params['sd_map'] = ArConfig.get('datastore_mapping','sd_map')
	pig_params['mongo_status_detail'] = 'mongodb://' + mongo_host + ':' + mongo_port + '/' + mongo_dest;
	
	cmd_pig = []

	# Append pig command
	cmd_pig.append('pig')

	# Append Pig local execution mode flag
	if ar_mode == "local":
		cmd_pig.append('-x')
		cmd_pig.append('local')

	# Append Pig Parameters
	for item in pig_params:
		cmd_pig.append('-param')
		cmd_pig.append(item+'='+pig_params[item])

	# Append Pig Executionable Script
	cmd_pig.append('-f')
	cmd_pig.append(pig_script_path+'compute-status.pig')

	try:
		check_call(cmd_pig)
		
	except Exception, err:
		sys.stderr.write('Error during execution of pig job \n')
		return 1


	print "Excution of job %s for tenant %s for date %s completed!" % (args.job , args.tenant, args.date)

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="Initiate a status detail job")
	arg_parser.add_argument("-d","--date",help="date", dest="date", metavar="DATE", required="TRUE")
	arg_parser.add_argument("-t","--tenant",help="tenant owner ", dest="tenant", metavar= "STRING", required="TRUE")
	arg_parser.add_argument("-j","--job",help="job name ", dest="job", metavar= "STRING", required="TRUE")
	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
