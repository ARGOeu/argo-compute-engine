#!/bin/env python
import pymongo
from pymongo import MongoClient

client = MongoClient()
client.write_concern = {'w': 0}
db = client.AR
db.drop_collection("timelines")

print "DB dropped"

collection = db.create_collection("timelines", size=1024*1024*1024*4,autoIndexId=False)

print "DB created"
