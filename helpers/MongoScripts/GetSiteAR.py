#!/bin/env python
import pymongo
from pymongo import MongoClient
import datetime
from datetime import datetime
from datetime import timedelta
from dateutil import parser
from time import strftime
import subprocess, sys, os, getopt

def addLogToDB(batch, input, dtnow):
    array = []
    mainhash = {
            datename: 0,
            site: 0,
            profile: 0,
            production: 0,
            monitored: 0,
            scope: 0,
            ngi: 0,
	    infrastructure: 0,
	    certification_status: 0,
	    site_scope: 0,
	    availability: 0,
	    reliability: 0
           }
    for line in input:
        items = line.rstrip().split('\001',11)
	mainhash[datename]             = dtnow
	mainhash[site]                 = items[0]
	mainhash[profile]              = items[1]
	mainhash[production]           = items[2]
	mainhash[monitored]            = items[3]
	mainhash[scope]                = items[4]
	mainhash[ngi]                  = items[5]
	mainhash[infrastructure]       = items[6]
	mainhash[certification_status] = items[7]
	mainhash[site_scope]           = items[8]
	mainhash[availability]         = float(items[9])
	mainhash[reliability]          = float(items[10])
        array.append(mainhash.copy())
        if len(array)%batch==0 :
            collection.insert(array,manipulate=False,safe=None,check_keys=False,w=0)
            print ("Inserted " + str(batch) +" entries")
            array = []
    collection.insert(array,manipulate=False,safe=None,check_keys=False,w=0)

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days + 1)):
	    yield start_date + timedelta(n)
        
#N = 2
#date = str(datetime.now() - timedelta(days=N)).split(" ")[0].split("-")

client = MongoClient()
client.write_concern = {'w': 0}
db = client.AR
collection = db.sites

datename             = "dt";
site                 = "s";
profile              = "p";
production           = "pr";
monitored            = "m";
scope                = "sc";
ngi                  = "n";
infrastructure       = "i";
certification_status = "cs";
site_scope           = "ss";
availability         = "a";
reliability          = "r";

start_date = parser.parse(sys.argv[1])
end_date   = parser.parse(sys.argv[2])

batch = 250000
for single_date in daterange(start_date, end_date):
    d = strftime("%Y-%m-%d", single_date.timetuple())
    print d
    date = d.split("-")
    subprocess.call(['hadoop', 'fs', '-get', '/user/hive/warehouse/sitereports/year=' + date[0] + '/month=' + date[1] + '/day=' + date[2] + '/part-r-*', 'part-r-0' + d])
    input = open('part-r-0' + d, "r")
    addLogToDB(batch, input, int("".join(date)));

os.system('rm -f part-r-*')


# s : "s", cs : "cs", i:"i", n:"n", m:"m", ss:"ss", p:"p", sc:"sc", pr:"pr"
