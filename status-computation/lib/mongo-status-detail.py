#!/usr/bin/env python

# Bulk insert status detaildata (probe messages) to MongoDB
# 
# example usage:
#
# Bulk insert for date 2014-10-23
#   ./mongo-status-detail.py 2014-10-23
#   
# Bulk insert (500 items per bulk) for date 2014-10-23:	
#   ./mongo-status-detail.py 2014-10-23 500

from pymongo import MongoClient
from ConfigParser import SafeConfigParser, Error
import datetime
import sys


bulk=50000 # number documents for each bulk insert into Mongo
cfg_fn = '/etc/ar-compute-engine.conf'
ar_consumer_path = '/var/lib/ar-consumer/'

if (len(sys.argv) < 2):
	print "missing date argument: (YY-MM-DD)"
	exit(1)

# if bulk is given as an argument use it
if (len(sys.argv) == 3):  
	bulk = int(sys.argv[2]) 

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
	print "ERROR: Could NOT parse %s because of following error:\n%s " % (cfg_fn, e.message)
	sys.exit(1)

# Open connection to mongodb
client = MongoClient(str(mongo_host),int(mongo_port))
db = client.AR
col = db['raw_status']

# Parse date argument given
arg_dt = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d")

# Remove mongodb records for specific date
print "Removing mongo entries for date: " + arg_dt.strftime("%Y-%m-%d")
db.raw_status.remove({"di":int(arg_dt.strftime("%Y%m%d"))})
print "Removed!"

# Insert new Records
print "Bulk insert..."
posts = [] # Posts list for bulk inserts
i = 0
tot = 0

# Open ar-consumer file
try: 
	with open (ar_consumer_path+"ar-consumer_log_details_"+arg_dt.strftime("%Y-%m-%d")+".txt") as f:
	    for line in f:
	    	i=i+1
	        tokens = line.split('\001') # Split each row into fields
	        dt = datetime.datetime.strptime(tokens[0], "%Y-%m-%dT%H:%M:%SZ") # Convert timestamp string to date
	        date_int = dt.year*10000 + dt.month*100 + dt.day
	        time_int = dt.hour*10000 + dt.minute*100 + dt.second

	        # Assemble json post
	        post = { "ts" : tokens[0],  # timestamp
	        "roc" : tokens[1],			# roc 
	        "mb" : tokens[2],			# monitoring box
	        "mn" : tokens[3],			# metric name
	        "sf" : tokens[4],			# service flavor
	        "sh" : tokens[5],			# service host
	        "s" : tokens[6],			# status
	        "vo" : tokens[7],			# vo 
	        "vof" : tokens[8],			# vo fqan
	        "sum" : tokens[9],			# summary
	        "msg" : tokens[10],			# nagios message
	        "di" : date_int,			# date as integer
	        "ti" : time_int }			# time as integer	
	        
	        # Add post to posts list for later insert
	        posts.append(post)
	        
	        # If bulk is ready push it to mongo
	        if (i==bulk):
	        	tot = tot + i
	        	col.insert(posts)
	        	posts=[]
	        	i=0
	        	print tot 

except (OSError, IOError) as e:
	print "ERROR: Could not find/parse %s \n " % (ar_consumer_path+"ar-consumer_log_details_"+arg_dt.strftime("%Y-%m-%d")+".txt")
	sys.exit(1)


# If posts remain in bulk, please insert         
if (len(posts)>0):
	col.insert(posts)

print "Bulk insert finished!"
