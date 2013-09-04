import pymongo
from pymongo import MongoClient
import time
from dateutil.tz import *
from datetime import *
import calendar
import Queue
import threading
from time import sleep
import os

queue = Queue.Queue();
utccc = tzutc();
def getmillis(stringdate):
    return calendar.timegm(datetime.strptime(stringdate , "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=tzutc()).utctimetuple())

          
class ThreadUrl(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        print("THREAD STARTED")
        client = MongoClient()
        client.write_concern = {'w': 0}
        db = client.AR
        collection = db.logs
        bulk = 250000
        array = []
        i=0 
        d = datetime;
        c = calendar;
        mainhash = { 
            datetim: 0,
            metric: 0,
            service_flavor: 0,
            hostname: 0,
            status: 0,
            vo: 0,
            fqan: 0,
            poem: 0
           }   
        sleep(1)
        while (not queue.empty()):
            #grabs host from quee
            line = self.queue.get()
            items = line.split('\001',8)
            mainhash[datetim] = items[0]
            mainhash[metric] = items[1]
            mainhash[service_flavor] = items[2]
            mainhash[hostname] = items[3]
            mainhash[status] = items[4]
            mainhash[vo] = items[5]
            mainhash[fqan] = items[6]
            mainhash[poem] = items[7]
            array.append(mainhash.copy())
            i+=1
            self.queue.task_done()
            #print(mainhash)
            if i%bulk==0 :
                i=0
                collection.insert(array,manipulate=False,safe=None,check_keys=False,w=0)
                print ("Inserted " + str(bulk) +" entries")
                array = []
	     #signals to queue job is done
        collection.insert(array,manipulate=False,safe=None,check_keys=False,w=0)
        array = [];
        exit(1)



def addLogToDB(filename):
    bulk = 250000
    ins = open( filename, "r")
    array = []
    i=0
    d = datetime;
    c = calendar;
    mainhash = {
            datetim: 0,
            metric: 0,
            service_flavor: 0,
            hostname: 0,
            status: 0,
            vo: 0,
            fqan: 0,
            poem: 0
           }
    print("STARTING THREADS")
    for i in range(3):
        t = ThreadUrl(queue)
        t.setDaemon(True)
        t.start()
    print("THREADS STARTED")
    for line in ins:
        queue.put(line)	
        #items = line.split('\001',8)
	#mainhash[datetim] = items[0]
	#mainhash[metric] = items[1]
	#mainhash[service_flavor] = items[2]
	#mainhash[hostname] = items[3]
	#mainhash[status] = items[4]
	#mainhash[vo] = items[5]
	#mainhash[fqan] = items[6]
	#mainhash[poem] = items[7]
        #array.append(mainhash.copy())
        #i+=1
        #if i%bulk==0 :
        #    i=0
        #    collection.insert(array,manipulate=False,safe=None,check_keys=False,w=0)
        #    print ("Inserted " + str(bulk) +" entries")
        #    array = []
    #collection.insert(array,manipulate=False,safe=None,check_keys=False,w=0)
    #array = [];
    queue.join()
print (pymongo.has_c());
client = MongoClient()
client.write_concern = {'w': 0}
db = client.AR
db.drop_collection("logs")
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
f = "./prefilter-2013-07-12.out"
addLogToDB(f);
queue.join();
