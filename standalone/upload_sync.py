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
	arcomp_conf = "/etc/ar-compute/"

	print args.date
	print args.tenant

	date_under = args.date.replace("-","_")

	print date_under

	ArConfig = SafeConfigParser()
	ArConfig.read(fn_ar_cfg)

	print ArConfig.get('default','mongo_host')
	print ArConfig.get('default','mongo_port')
	print ArConfig.get('default','serialization')
	
	# gather needed sync files
	fn_egroups = 'group_endpoints_' + date_under + '.avro'
	fn_ggroups = 'group_groups_' + date_under + '.avro'
	fn_downtimes = 'downtimes_' + args.date + '.avro'
	fn_weights = 'weights_sync_'+ date_under + '.avro'
	fn_mps = 'poem_sync_' + date_under + '.avro'
	fn_aps = args.tenant + '_' + args.job + '_ap.json'
	fn_cfg = args.tenant + '_' + args.job + '_cfg.json'
	fn_rec = args.tenant + '_recalc.json'

	# hdfs destination
	hdfs_dest = './' + args.tenant + '/sync/' + args.job + '/'

	arsync_job = arsync_lib + args.tenant + '/' + args.job + '/'

	print os.path.join(arsync_job,fn_mps)
	print os.path.join(arsync_job,fn_egroups)
	print os.path.join(arsync_job,fn_ggroups)
	print os.path.join(arsync_job,fn_weights)
	print os.path.join(arsync_lib,fn_downtimes)
	print os.path.join(arcomp_conf,fn_aps)
	print os.path.join(arcomp_conf,fn_cfg)
	print os.path.join(arcomp_conf,fn_rec)
	print hdfs_dest

	cmd_clearHdfs = ['hadoop','fs','-rm','-f',hdfs_dest+"*"]
	cmd_putMps =  ['hadoop','fs','-put','-f',os.path.join(arsync_job,fn_mps),hdfs_dest]
	cmd_putEgroups =  ['hadoop','fs','-put','-f',os.path.join(arsync_job,fn_egroups),hdfs_dest]
	cmd_putGgroups = ['hadoop','fs','-put','-f',os.path.join(arsync_job,fn_ggroups),hdfs_dest]
	cmd_putWeights = ['hadoop','fs','-put','-f',os.path.join(arsync_job,fn_weights),hdfs_dest]
	cmd_putDowntimes = ['hadoop','fs','-put','-f',os.path.join(arsync_lib,fn_downtimes),hdfs_dest]
	cmd_putAps = ['hadoop','fs','-put','-f',os.path.join(arcomp_conf,fn_aps),hdfs_dest]
	cmd_putCfg = ['hadoop','fs','-put','-f',os.path.join(arcomp_conf,fn_cfg),hdfs_dest]
	cmd_putRec = ['hadoop','fs','-put','-f',os.path.join(arcomp_conf,fn_rec),hdfs_dest]
	
	try:
		check_call(cmd_clearHdfs)
		check_call(cmd_putMps)
		check_call(cmd_putEgroups)
		check_call(cmd_putGgroups)
		check_call(cmd_putWeights)
		check_call(cmd_putDowntimes)
		check_call(cmd_putAps)
		check_call(cmd_putCfg)
		check_call(cmd_putRec)
	except Exception, err:
		
		sys.stderr.write('Could not upload sync data to hdfs \n')
		return 1

	print "Sync Data of tenant %s for job %s for date %s uploaded successfully to hdfs" % (args.tenant , args.job, args.date)

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="Uploading sync data to hdfs")
	arg_parser.add_argument("-d","--date",help="raw text input file", dest="date", metavar="DATE", required="TRUE")
	arg_parser.add_argument("-t","--tenant",help="output avro file ", dest="tenant", metavar= "STRING", required="TRUE")
	arg_parser.add_argument("-j","--job",help="output avro file ", dest="job", metavar= "STRING", required="TRUE")

	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
