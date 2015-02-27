#!/usr/bin/env python

# arg parsing related imports
import os, sys
from subprocess import call
from argparse import ArgumentParser
from ConfigParser import SafeConfigParser

def main(args=None):

	# Default core paths 
	fn_ar_cfg = "/etc/ar-compute-engine.conf"
	arsync_exec = "/usr/libexec/ar-sync/"
	arsync_lib = "/var/lib/ar-sync/"
	arcomp_conf = "/etc/ar-compute/"

	# Create a second date used by the file formats
	date_under = args.date.replace("-","_")

	# Initiate config file parser to read global ar-compute-engine.conf 
	ArConfig = SafeConfigParser()
	ArConfig.read(fn_ar_cfg)
	
	# Compose needed sync filenames using the correct prefixes, dates and file extensions (avro/json)
	fn_egroups = 'group_endpoints_' + date_under + '.avro'
	fn_ggroups = 'group_groups_' + date_under + '.avro'
	fn_downtimes = 'downtimes_' + args.date + '.avro'
	fn_weights = 'weights_sync_'+ date_under + '.avro'
	fn_mps = 'poem_sync_' + date_under + '.avro'
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

	# Compose the local paths for files (paths+filenames)
	local_egroups = os.path.join(arsync_job,fn_egroups)
	local_ggroups = os.path.join(arsync_job,fn_ggroups)
	local_downtimes = os.path.join(arsync_lib,fn_downtimes)
	local_weights = os.path.join(arsync_lib,fn_weights)
	local_mps = os.path.join(arsync_job,fn_mps)
	local_aps = os.path.join(arcomp_conf,fn_aps)
	local_ops = os.path.join(arcomp_conf,fn_ops)
	local_cfg = os.path.join(arcomp_conf,fn_cfg)
	local_rec = os.path.join(arcomp_conf,fn_rec)

	# Check filenames if exist
	print "Check if %s exists: %s" % (local_egroups,os.path.exists(local_egroups))
	print "Check if %s exists: %s" % (local_ggroups,os.path.exists(local_ggroups))
	print "Check if %s exists: %s" % (local_downtimes,os.path.exists(local_downtimes))
	print "Check if %s exists: %s" % (local_weights,os.path.exists(local_weights))
	print "Check if %s exists: %s" % (local_mps,os.path.exists(local_mps))
	print "Check if %s exists: %s" % (local_aps,os.path.exists(local_aps))
	print "Check if %s exists: %s" % (local_ops,os.path.exists(local_ops))
	print "Check if %s exists: %s" % (local_cfg,os.path.exists(local_cfg))
	print "Check if %s exists: %s" % (local_rec,os.path.exists(local_rec))
	

	# Remove scratch sync directory in hdfs (cause we don't keep unarchived sync files)
	cmd_clearHdfs = ['hadoop','fs','-rm','-r',hdfs_dest]
	# Establish new scratch sync directory in hdfs for this job
	cmd_estHdfs = ['hadoop','fs','-mkdir','-p',hdfs_dest]
	# Transfer endpoint groups topo from local to hdfs
	cmd_putEgroups =  ['hadoop','fs','-put','-f',local_egroups,hdfs_dest]
	# Transfer group of groups topo from local to hdfs 
	cmd_putGgroups = ['hadoop','fs','-put','-f',local_ggroups,hdfs_dest]
	# Transfer weight factors from local to hdfs
	cmd_putWeights = ['hadoop','fs','-put','-f',local_weights,hdfs_dest]
	# Transfer downtime info from local to hdfs
	cmd_putDowntimes = ['hadoop','fs','-put','-f',local_downtimes,hdfs_dest]
	# Transfer metric profile from local to hdfs
	cmd_putMps =  ['hadoop','fs','-put','-f',local_mps,hdfs_dest]
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
	print "Transfer avaliability profile"
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
