#!/usr/bin/env pythn

import logging 
import logging.handlers

def prepare_logger(log_file,log_level,log_name):
	# Instantiate logger with proper name
	logger = logging.getLogger(log_name)
	logger.setLevel(logging.DEBUG)
	# Define two handlers (file / console)
	#file_log = logging.FileHandler(log_file)
	cmd_log = logging.StreamHandler()
	sys_log = logging.handlers.SysLogHandler("/dev/log")
	# The log level for console is always up to info
	cmd_log.setLevel(logging.INFO)

	# The log level for file is parametized
	if log_level == 'DEBUG' :
		sys_log.setLevel(logging.DEBUG)
	elif log_level == 'INFO':
		sys_log.setLevel(logging.INFO)
	elif log_level == 'WARNING':
		sys_log.setLevel(logging.WARNING)
	elif log_level == 'ERROR':
		sys_log.setLevel(logging.ERROR)
	elif log_level == 'CRITICAL':
		sys_log.setLevel(logging.CRITICAL)
	# Define the format for the file log formatting
	sys_format = logging.Formatter('%(name)s %(levelname)s %(message)s')
	sys_log.setFormatter(sys_format)
	# Add handlers 
	logger.addHandler(sys_log)
	logger.addHandler(cmd_log)

	return logger