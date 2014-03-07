#!/bin/env python
from pymongo import MongoClient
from ConfigParser import SafeConfigParser
import sys

#read arguments from configuration file using standar library's configParser
cfg_filename = '/etc/ar-compute-engine.conf'
cfg_parser = SafeConfigParser()

# Try parsing - if file exists but parsing fails will throw exception
try:
	# check if file exists while trying to parse it
	if (len(cfg_parser.read(cfg_filename)) == 0):
		print "Configuration File:%s not found! \nExiting..." % cfg_filename
		sys.exit(1)
except ConfigParser.Error as e:
	print "Configuration File Parse Error: %s \nExiting..." % e.message
	sys.exit(1)

# Now we are free to grab values without worrying about lines, spaces and comments in file
mongo_host = cfg_parser.get('default','mongo_host')
mongo_port = cfg_parser.get('default','mongo_port')

client = MongoClient(str(mongo_host), int(mongo_port))
db = client.AR
date = sys.argv[1].replace("-","")

db.sites.remove({"dt": date})
db.timelines.remove({"d": date})
db.voreports.remove({"d": date})
db.sfreports.remove({"d": date})
