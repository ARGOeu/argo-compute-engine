#!/bin/env python
from pymongo import MongoClient
import sys

mongo_host = sys.argv[2]
mongo_port = sys.argv[3]

client = MongoClient(str(mongo_host), mongo_port)
db = client.AR
date = sys.argv[1].replace("-","")

db.sites.remove({"dt": date})
db.timelines.remove({"d": date})
db.voreports.remove({"d": date})
db.sfreports.remove({"d": date})
