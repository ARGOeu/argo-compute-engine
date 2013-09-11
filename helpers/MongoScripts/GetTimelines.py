#!/bin/env python
import pymongo
from pymongo import MongoClient
import datetime
from datetime import datetime
from datetime import timedelta
from time import strftime
from dateutil import parser
import subprocess, sys, os, getopt

def addLogToDB(batch, input, date_int):
    array = []
    mainhash = {
            datetim: 0,
            service_flavor: 0,
            hostname: 0,
            vo: 0,
            profile: 0,
            namespace: 0,
            timeline: 0
           }
    for line in input:
        items = line.split('\001',5)
	mainhash[datetim] = date_int
	mainhash[hostname] = items[0]
	mainhash[service_flavor] = items[1]
	mainhash[namespace] = items[2][0:items[2].rindex('.')]
	mainhash[profile] = items[2][items[2].rindex('.')+1:]
	mainhash[vo] = items[3]
	mainhash[timeline] = items[4]
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

start_date = parser.parse(sys.argv[1])
end_date   = parser.parse(sys.argv[2])

client = MongoClient()
client.write_concern = {'w': 0}
db = client.AR
collection = db.timelines

datetim        = "d"; 
service_flavor = "sf"; 
hostname       = "h"; 
vo             = "vo"; 
namespace      = "ns";
profile        = "p"; 
timeline       = "tm";

batch = 250000

for single_date in daterange(start_date, end_date):
    d = strftime("%Y-%m-%d", single_date.timetuple())
    print d
    date = d.split("-")
    subprocess.call(['hadoop', 'fs', '-get', '/user/hive/warehouse/apireports/year=' + date[0] + '/month=' + date[1] + '/day=' + date[2] + '/part-r-*'])
    os.system('cat part-r-* > apiinput')
    input = open('apiinput', "r")
    addLogToDB(batch, input, int("".join(date)))
    os.system('rm -f part-r-*')

os.system('rm -f apiinput')
