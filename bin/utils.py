#!/usr/bin/env python
import json
from ConfigParser import SafeConfigParser
from collections import defaultdict
from datetime import datetime, timedelta


class ArgoConfiguration(object):
    """Util class to retrieve and hold Enviroment Configuration"""
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
    # connector parameters
    sync_exec = None
    sync_path = None
    # consumers parmeters
    consumers_root = None
    # ce run mode
    mode = None
    # sampling
    sampling_period = None
    sampling_interval = None
    # clean
    sync_clean = None
    prefilter_clean = None
    # tenant ar store configuration
    tenant_db_conf = {}
    # recomputation threshold
    threshold = None

    def __init__(self, filename):
        self.load_config(filename)

    def load_config(self, filename):
        """
        Load configuration from filename.
        Create tenant list
        Create tenant/jobs dictionary

        :param filename: Config filename to load information from
        """
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

        # Grab connector sync path
        self.sync_exec = ar_config.get('connectors', 'sync_exec')
        self.sync_path = ar_config.get('connectors', 'sync_path')
        self.consumers_root = ar_config.get('consumers','consumers_root')

        # Grab run mode
        self.mode = ar_config.get('default', 'mode')

        # Grab sampling parameters
        self.sampling_period = ar_config.get('sampling', 's_period')
        self.sampling_interval = ar_config.get('sampling', 's_interval')

        # Grab clean parameters
        self.prefilter_clean = ar_config.get('default', 'prefilter_clean')
        self.sync_clean = ar_config.get('default', 'sync_clean')

        self.threshold = int(ar_config.get('default', 'recomp_threshold'))

    def load_tenant_db_conf(self, filename):
        """
        Load tenant db configuration and return it as a dictionary

        :param filename: path to json file containing tenant db configuration
        :returns: dictionary with database configuration
        """
        self.tenant_db_conf = {}

        with open(filename) as json_file:
            json_data = json.load(json_file)

        for item in json_data["db_conf"]:
            self.tenant_db_conf[item["store"]] = item

    def get_mongo_uri(self, store, collection):
        store_cfg = self.tenant_db_conf[store]
        if store_cfg["username"] and store_cfg["password"]:
            mongo_uri = ["mongodb://", store_cfg["username"], ":", store_cfg["password"],
                         "@", store_cfg["server"], ":", str(store_cfg["port"]),
                         "/", store_cfg["database"], ".", collection]
        else:
            mongo_uri = ["mongodb://", store_cfg["server"], ":", str(store_cfg["port"]),
                         "/", store_cfg["database"], ".", collection]

        return "".join(mongo_uri)

    def get_mongo_database(self, store):
        return self.tenant_db_conf[store]["database"]

    def get_mongo_host(self, store):
        return self.tenant_db_conf[store]["server"]

    def get_mongo_port(self, store):
        return self.tenant_db_conf[store]["port"]


def get_date_under(date):
    """
    Convert date str format from dashes to underscores

    :param date: string using format (YYYY-MM-DD)
    :returns: string using format (YYYY_MM_DD)
    """
    return date.replace("-", "_")


def get_actual_date(date):
    """
    Convert date str to actual python date object

    :param date: string using format (YYYY-MM-DD)
    :returns: python date object
    """
    return datetime.strptime(date, '%Y-%m-%d')


def get_date_str(date):
    """
    Convert date object to date string (YYYY-MM-DD)

    :param date: python date object
    :returns: string using format (YYYY-MM-DD)
    """
    return date.strftime('%Y-%m-%d')


def get_date_range(start_date, end_date):
    """
    Create a list of all dates included in a specified time period

    :param start_date: date string (YYYY-MM-DD)
    :param end_date: date string (YYYY-MM-DD)
    :returns: list of dates
    """
    ta = get_actual_date(start_date)
    tb = get_actual_date(end_date)
    if tb < ta:
        raise ValueError('Time period is negative (end date comes before start date)')
    delta = tb - ta
    dates = [ta]
    for i in range(1, delta.days):
        tc = ta + timedelta(days=i)
        dates.append(tc)
    if tb > ta:
        dates.append(tb)

    return dates
