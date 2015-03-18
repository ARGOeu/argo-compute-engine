#!/bin/env python
import argparse, sys, os
from subprocess import check_call
from datetime import datetime, timedelta
from ConfigParser import SafeConfigParser

# Generate a series of the wanted dates
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)

# A custom function to change the parsed string into a date object
class StringToDate(argparse.Action):
    def __call__(self, parser, namespace, value, option_string=None):
        if not value:
            return
        setattr(namespace, self.dest, datetime.strptime(value, '%Y-%m-%d').date())

# main
def main(args=None):
    work_path = '/usr/libexec/ar-compute/lib/'
    prefilter_path = '/usr/libexec/ar-sync/'
    #read arguments from configuration file using standar library's configParser
    cfg_filename = '/etc/ar-compute-engine.conf'
    cfg_parser = SafeConfigParser()
    
    # Try parsing - if file exists but parsing fails will throw exception
    try:
        # check if file exists while trying to parse it
        if (len(cfg_parser.read(cfg_filename)) == 0):
            print "Configuration File:%s not found! \nExiting..." % cfg_filename
            return(1)
    except ConfigParser.Error as e:
        print "Configuration File Parse Error: %s \nExiting..." % e.message
        return(1)
    
    # Calculation mode: local/cluster
    calculation_mode = cfg_parser.get('default','mode')
    # MongoDB info
    mongo_host = cfg_parser.get('default','mongo_host')
    mongo_port = cfg_parser.get('default','mongo_port')
    
    # Check if the user provided only 1 date
    if not args.end_date:
        args.end_date = args.start_date

    # For each day
    for single_date in daterange(args.start_date, args.end_date):
        running_date_str = datetime.strftime(single_date, '%Y-%m-%d')
        
        if not args.no_input and calculation_mode == 'cluster':
            check_call([os.path.join(work_path, 'send-raw-data.sh'), running_date_str])
        
        if not args.no_input and calculation_mode == 'local':
            check_call([os.path.join(prefilter_path, 'prefilter'), '-d', running_date_str])

        cmd = [os.path.join(work_path, 'calculate.sh'), running_date_str, mongo_host+":"+mongo_port]
        
        if calculation_mode == 'local':
            cmd.extend(["-x local", "local_"])

        check_call(cmd)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", action=StringToDate, help="The first date of the calculation")
    parser.add_argument("end_date", nargs='?', action=StringToDate, help="The last date of the calculation (This argument is optional)")
    # If the -ni flag exists, we skip the input phase
    parser.add_argument("-ni", "--no_input", default=False, action="store_true", help="Skip the input phase (only on cluster mode)")
    
    sys.exit(main(parser.parse_args()))
