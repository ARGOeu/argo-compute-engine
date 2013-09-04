#!/bin/env python
import pymongo
from pymongo import MongoClient
import time
from dateutil.tz import *
from datetime import *
import calendar
import Queue
import threading
from time import sleep

import subprocess
import glob
import os
import sys

utccc = tzutc();
def getmillis(stringdate):
    return calendar.timegm(datetime.strptime(stringdate , "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=tzutc()).utctimetuple())
         
def addLogToDB(filename):
    bulk = 250000
    ins = open( filename, "r")
    array = []
    i=0
    d = datetime;
    c = calendar;
    mainhash = {
            datetim: 0,
            service_flavor: 0,
            hostname: 0,
            vo: 0,
            profile: 0,
            timeline: 0
           }
    for line in ins:
        items = line.split('\001',8)
	#mainhash[datetim] = c.timegm(d.strptime(items[0],"%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=utccc).utctimetuple())
	mainhash[datetim] = int(date[0] + date[1] + date[2])
	mainhash[hostname] = items[0]
	mainhash[service_flavor] = items[1]
	mainhash[profile] = items[2]
	mainhash[vo] = items[3]
	mainhash[timeline] = items[4]
        array.append(mainhash.copy())
        i+=1
        if i%bulk==0 :
            i=0
            collection.insert(array,manipulate=False,safe=None,check_keys=False,w=0)
            print ("Inserted " + str(bulk) +" entries")
            array = []
    collection.insert(array,manipulate=False,safe=None,check_keys=False,w=0)
    array = [];

# print (pymongo.has_c());
N = 2
date_N_days_ago = datetime.now() - timedelta(days=N)
date = str(date_N_days_ago).split(" ")[0].split("-")

subprocess.call(['hadoop', 'fs', '-get', '/user/hive/warehouse/apireports/year=' + date[0] + '/month=' + date[1] + '/day=' + date[2] + '/part-r-*'])

os.system('cat part-r-* > apiinput')
os.system('rm -f part-r-*')

client = MongoClient()
client.write_concern = {'w': 0}
db = client.AR
#db.drop_collection("logs")
collection = db.create_collection("logs", size=1024*1024*1024*4,autoIndexId=False)
collection = db.logs
bulk = 25000

datetim = "dt"; 
metric = "m"; 
service_flavor = "sf"; 
hostname = "h"; 
status = "s"; 
vo = "vo"; 
fqan = "fq"; 
poem = "pm";
file = "./apiinput"
addLogToDB(file);

os.system('rm -f apiinput')
