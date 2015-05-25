#!/usr/bin/env python

from ConfigParser import SafeConfigParser

class ArgoConfiguration(object):

    db_name = None
    rec_col = None
    mongo_host = None
    mongo_port = None
    log_mode = None
    log_file = None
    tenant = None
    date = None
    date_under = None

    def __init__(self,args,filename):
        self.load_args(args)
        self.load_config(filename)

    def load_args(self,args):
        # Read the arguments from argparser
        self.tenant = args.tenant
        self.date = args.date 
        # Create also date_under parameter
        self.date_under = args.date.replace("-", "_")

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
