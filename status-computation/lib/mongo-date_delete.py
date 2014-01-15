#!/bin/env python
from pymongo import MongoClient
import sys

client = MongoClient('83.212.110.19', 27017)
db = client.AR
collection1 = db.sites
collection2 = db.timelines
date = sys.argv[1].replace("-","")

collection1.remove({"dt": date})
collection2.remove({"d": date})
