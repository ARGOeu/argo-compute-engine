#!/usr/bin/env python

from ConfigParser import SafeConfigParser
from datetime import datetime, timedelta

class ArgoConfiguration(object):

    # Configuration parameters
    mongo_host = None
    mongo_port = None
    log_mode = None
    log_file = None
    log_level = None

    def __init__(self,filename):

        self.load_config(filename)

    def load_config(self,filename):
        # Init Config parser
        ArConfig = SafeConfigParser()
        # Read conf file
        ArConfig.read(filename)
        # Grab the conf parameters
        self.mongo_host = ArConfig.get('default', 'mongo_host')
        self.mongo_port = ArConfig.get('default', 'mongo_port')
        self.log_mode = ArConfig.get('logging', 'log_mode')
        if self.log_mode == 'file':
            self.log_file = ArConfig.get('logging', 'log_file')
        self.log_level = ArConfig.get('logging','log_level')

def get_date_under(date):
    return date.replace("-", "_")

def get_actual_date(date):
    return  datetime.strptime(date, '%Y-%m-%d')

def get_date_str(date):
    return date.strftime('%Y-%m-%d')

def get_date_range(start_date,end_date):
    ta = get_actual_date(start_date)
    tb = get_actual_date(end_date)
    delta = tb - ta
    dates = []
    dates.append(ta)
    for i in range (1,delta.days):
        tc = ta + timedelta(days=i)
        dates.append(tc)
    dates.append(tb)

    return dates