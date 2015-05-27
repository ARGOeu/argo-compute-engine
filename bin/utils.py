#!/usr/bin/env python

from ConfigParser import SafeConfigParser
from collections import defaultdict
from datetime import datetime, timedelta


class ArgoConfiguration(object):
    # Datastore parameters
    mongo_host = None
    mongo_port = None
    # Logging parameters
    log_mode = None
    log_file = None
    log_level = None
    # Tenant parameters
    tenants = []
    jobs = defaultdict(list)

    def __init__(self, filename):

        self.load_config(filename)

    def load_config(self, filename):
        # Init Config parser
        ar_config = SafeConfigParser()
        # Read conf file
        ar_config.read(filename)
        # Grab Datastore Configuration
        self.mongo_host = ar_config.get('default', 'mongo_host')
        self.mongo_port = ar_config.get('default', 'mongo_port')
        # Grab Logging Configuration
        self.log_mode = ar_config.get('logging', 'log_mode')
        if self.log_mode == 'file':
            self.log_file = ar_config.get('logging', 'log_file')
        self.log_level = ar_config.get('logging', 'log_level')
        # Grab tenant Configuration
        tenant_list = ar_config.get("jobs", "tenants")
        tenant_list = tenant_list.split(',')
        for tenant_item in tenant_list:
            self.tenants.append(tenant_item)

            # Get specific tenant's job set
            job_set = ar_config.get("jobs", tenant_item + '_jobs')
            job_set = job_set.split(',')
            for job_item in job_set:
                self.jobs[tenant_item].append(job_item)


def get_date_under(date):
    return date.replace("-", "_")


def get_actual_date(date):
    return datetime.strptime(date, '%Y-%m-%d')


def get_date_str(date):
    return date.strftime('%Y-%m-%d')


def get_date_range(start_date, end_date):
    
    ta = get_actual_date(start_date)
    tb = get_actual_date(end_date)
    if tb<ta:
        raise ValueError('Time period is negative (end date comes before start date)')
    delta = tb - ta
    dates = [ta]
    for i in range(1, delta.days):
        tc = ta + timedelta(days=i)
        dates.append(tc)
    if tb>ta:
        dates.append(tb)

    return dates
