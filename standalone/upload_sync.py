#!/usr/bin/env python

# arg parsing related imports
import os, sys
from datetime import datetime, timedelta
from subprocess import call
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser


def getSyncFile(dt,prefix,postfix,splitstr):
	days_back=0
	while True:
		target_dt = dt - timedelta(days=days_back)
		date_split = target_dt.strftime('%Y'+splitstr+'%m'+splitstr+'%d')
		file_path = prefix + date_split + postfix
		
		print "Check if %s exists..." % file_path

		if (os.path.exists(file_path)):
			print "True"
			return file_path
		else:
			days_back = days_back + 1
			print "False, try %s days back" % str(days_back)

		if days_back > 3:
			print "ERROR: Too many days without a file..."
			sys.exit(1)




def main(args=None):

	# Default core paths 
	fn_ar_cfg = "/etc/ar-compute-engine.conf"
	arsync_exec = "/usr/libexec/ar-sync/"
	arsync_lib = "/var/lib/ar-sync/"
	arcomp_conf = "/etc/ar-compute/"

	actual_date = datetime.strptime(args.date,'%Y-%m-%d')

	# Create a second date used by the file formats
	date_under = args.date.replace("-","_")

	# Initiate config file parser to read global ar-compute-engine.conf 
	ArConfig = SafeConfigParser()
	ArConfig.read(fn_ar_cfg)
	
	# Compose needed sync filenames using the correct prefixes, dates and file extensions (avro/json)

	fn_ops = args.tenant + '_ops.json'
	fn_aps = args.tenant + '_' + args.job + '_ap.json'
	fn_cfg = args.tenant + '_' + args.job + '_cfg.json'
	fn_rec = args.tenant + '_recalc.json'

	# compose hdfs temporary destination
	# hdfs dest = ./tenant/sync/job/date/...
	# sync files are not meant to be kept in hdfs (unless archived in batches)
	hdfs_dest = './scratch/sync/' + args.tenant + '/' + args.job + '/' + date_under + '/'
	
	# Compose the local ar-sync files job folder 
	# arsync job = /var/lib/ar-sync/tenant/job/...
	arsync_job = arsync_lib + args.tenant + '/' + args.job + '/'


	# Call downtimes latest info
	cmd_call_downtimes = [os.path.join(arsync_exec,'downtime-sync'),'-d',args.date]
	print "Calling downtime sync to give us latest downtime info"
	call(cmd_call_downtimes)

	# Compose the local paths for files (paths+filenames)
	local_egroups = getSyncFile(actual_date, os.path.join(arsync_job,"group_endpoints_") , '.avro','_')
	local_ggroups = getSyncFile(actual_date, os.path.join(arsync_job,"group_groups_") , '.avro','_')
	local_weights = getSyncFile(actual_date, os.path.join(arsync_job,"weights_sync_") , '.avro','_')
	local_mps = getSyncFile(actual_date, os.path.join(arsync_job,"poem_sync_"), '.avro','_')
	local_downtimes = getSyncFile(actual_date, os.path.join(arsync_lib,"downtimes_"), '.avro','-')

	local_aps = os.path.join(arcomp_conf,fn_aps)
	local_ops = os.path.join(arcomp_conf,fn_ops)
	local_cfg = os.path.join(arcomp_conf,fn_cfg)
	local_rec = os.path.join(arcomp_conf,fn_rec)

	# Check filenames if exist
	print "Check if %s exists: %s" % (local_aps,os.path.exists(local_aps))
	print "Check if %s exists: %s" % (local_ops,os.path.exists(local_ops))
	print "Check if %s exists: %s" % (local_cfg,os.path.exists(local_cfg))
	print "Check if %s exists: %s" % (local_rec,os.path.exists(local_rec))
	

	# Remove scratch sync directory in hdfs (cause we don't keep unarchived sync files)
	cmd_clearHdfs = ['hadoop','fs','-rm','-r',hdfs_dest]
	# Establish new scratch sync directory in hdfs for this job
	cmd_estHdfs = ['hadoop','fs','-mkdir','-p',hdfs_dest]
	# Transfer endpoint groups topo from local to hdfs
	cmd_putEgroups =  ['hadoop','fs','-put','-f',local_egroups,hdfs_dest+'group_endpoints.avro']
	# Transfer group of groups topo from local to hdfs 
	cmd_putGgroups = ['hadoop','fs','-put','-f',local_ggroups,hdfs_dest+'group_groups.avro']
	# Transfer weight factors from local to hdfs
	cmd_putWeights = ['hadoop','fs','-put','-f',local_weights,hdfs_dest+'weights_sync.avro']
	# Transfer metric profile from local to hdfs
	cmd_putMps =  ['hadoop','fs','-put','-f',local_mps,hdfs_dest+'poem_sync.avro']
	# Transfer downtime info from local to hdfs
	cmd_putDowntimes = ['hadoop','fs','-put','-f',local_downtimes,hdfs_dest+'downtimes.avro']
	
	# Transfer availability profile from local to hdfs
	cmd_putAps = ['hadoop','fs','-put','-f',local_aps,hdfs_dest]
	# Transfer operations from local to hdfs 
	cmd_putOps = ['hadoop','fs','-put','-f',local_ops,hdfs_dest]
	# Transfer job configuration file from local to hdfs
	cmd_putCfg = ['hadoop','fs','-put','-f',local_cfg,hdfs_dest]
	# Transfer recalculations requests (if any) from local to hdfs
	cmd_putRec = ['hadoop','fs','-put','-f',local_rec,hdfs_dest]
	
	#try:
	print "Remove old hdfs scratch sync folder: %s" % hdfs_dest
	call(cmd_clearHdfs)
	print "Establish new hdfs scratch sync folder %s" % hdfs_dest
	call(cmd_estHdfs)
	print "Transfer metric profile"
	call(cmd_putMps)
	print "Transfer endpoint group topology"
	call(cmd_putEgroups)
	print "Transfer group of group topology"
	call(cmd_putGgroups)
	print "Transfer weight factors"
	call(cmd_putWeights)
	print "Transfer downtimes"
	call(cmd_putDowntimes)
	print "Transfer availability profile"
	call(cmd_putAps)
	print "Transfer operations file"
	call(cmd_putOps)
	print "Transfer job configuration"
	call(cmd_putCfg)
	print "Transfer recalculation requests"
	call(cmd_putRec)
	

	print "Sync Data of tenant %s for job %s for date %s uploaded successfully to hdfs" % (args.tenant , args.job, args.date)

if __name__ == "__main__":

	# Feed Argument parser with the description of the 3 arguments we need (input_file,output_file,schema_file)
	arg_parser = ArgumentParser(description="Uploading sync data to hdfs")
	arg_parser.add_argument("-d","--date",help="date", dest="date", metavar="DATE", required="TRUE")
	arg_parser.add_argument("-t","--tenant",help="tenant owner", dest="tenant", metavar= "STRING", required="TRUE")
	arg_parser.add_argument("-j","--job",help="job name", dest="job", metavar= "STRING", required="TRUE")

	# Parse the command line arguments accordingly and introduce them to main...
	sys.exit(main(arg_parser.parse_args()))
