#!/bin/env python
from pymongo import MongoClient
import sys

client = MongoClient('83.212.110.19', 27017)
db = client.AR
date = sys.argv[1].replace("-","")

db.sites.remove({"dt": date})
db.timelines.remove({"d": date})
db.voreports.remove({"d": date})
db.sfreports.remove({"d": date})
