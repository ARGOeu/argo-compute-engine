#!/usr/bin/env python

# Retrieve Distinct Poem List information from poem_sync files
# 
# example usage:
#
# Retrieve poem info from latest poem_sync:
#	./gen_poem_list 
#   
# Retrieve poem info from a specific date:	
#   ./gen_poem_list 2012-12-24

import sys, os, fnmatch, datetime
from pymongo import MongoClient
from ConfigParser import SafeConfigParser, Error

#enviroment: ar-sync path and ar-compute config
ar_sync_path = '/var/lib/ar-sync/'
cfg_fn = '/etc/ar-compute-engine.conf'

#Determine the date
try:
	#Check to see if date is given as an argument
	if len(sys.argv) == 1:
		print "No date specified will use latest poem_sync"
		#List files in ar-sync directory and retrieve max date
		max = 0
		for file in os.listdir(ar_sync_path):
			if fnmatch.fnmatch(file, 'poem*.out'):
				tmp_int = int(file[-14:-4].replace('_',''))
				if max < tmp_int:
					max = tmp_int

		#If max still unchanged no poem files found
		if max == 0:
			print "Error: No poem_sync files found!"
			sys.exit(1)

		#Parse max integer as date
		poem_date = datetime.datetime.strptime(str(max),'%Y%m%d')	
	else:
		
		poem_date = datetime.datetime.strptime(sys.argv[1],'%Y-%m-%d')

except ValueError:
	print "Wrong Date Format!\nShould be: YYYY-MM-DD\nfor eg:\n   %s 2012-12-24" % sys.argv[0]
	sys.exit(1)

# compose complete poem_sync filename
poem_fn = ''.join([ar_sync_path,'poem_sync_',poem_date.strftime("%Y_%m_%d"),'.out'])

#Check if poemfile exists for that date
if (os.path.isfile(poem_fn) == False):
	print "Looking for:\n   %s\nPoem does NOT exist for such date" % poem_fn
	sys.exit(1)

#Read Enviroment Configuration
parser = SafeConfigParser()

#Try to open and parse the configuration file 
try:
	 
	if not (len(parser.read(cfg_fn))):
	 	print "Cannot load config\nEnsure %s exists!" % cfg_fn
	 	sys.exit(1)

	mongo_host = parser.get('default','mongo_host')
	mongo_port = parser.get('default','mongo_port')

except Error as e:
	print "Could NOT parse %s because of following error:\n%s " % (cfg_fn, e.message)
	sys.exit(1)

#use python Set to keep distinct names
poem_set = set()

# Open poem file and tokenize each line (using \x01 delim) 
# poem profile name lies in third token/column

with open(poem_fn) as poem_fl:
	for line in poem_fl:
		tokn = line.split('\x01')
		poem_set.add(tokn[2])
		
print "Enviroment OK\nRequested Date: %s\nRequested Poem File:%s" %(poem_date,poem_fn)


try:
	#Connect to mongo and use AR database, clear and store
	mongo_cl = MongoClient(str(mongo_host),int(mongo_port))
	mongo_db = mongo_cl.AR
	mongo_db.poem_list.remove()

	print "Connected to MongoDB\nCleared the previous results"

	print "Fresh Poems Gathered and Stored in Mongo:"
	
	for i, item in enumerate(poem_set):
		print "   %s" % item
		mongo_db.poem_list.insert({"p" : item})
	
	print "Total (%d)" % (i+1)


except Exception, e:
	print "Trouble with MongoDB because of following error:\n%s" % e.message
	sys.exit(1)

