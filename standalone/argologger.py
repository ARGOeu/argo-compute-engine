#!/usr/bin/env pythn

import logging 
import logging.handlers

def init_logger(log_mode,log_file,log_level,log_name):
	# Instantiate logger with proper name
	logger = logging.getLogger(log_name)
	logger.setLevel(logging.DEBUG)

	# Instantiate default levels
	levels = { 
		'DEBUG':logging.DEBUG,
		'INFO':logging.INFO,
		'WARNING':logging.WARNING,
		'ERROR':logging.ERROR,
		'CRITICAL':logging.CRITICAL
	}	

	# Logger should always have a stream handler to display messages to console
	cmd_log = logging.StreamHandler()
	cmd_log.setLevel(logging.INFO)
	logger.addHandler(cmd_log)

	# If log_mode = file then setup a file handler
	if log_mode == 'file':
		
		file_log = logging.FileHandler(log_file)
		file_format = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
		file_log.setFormatter(file_format)
		
		logger.addHandler(file_log)

	# If log_mode = syslog then setup a syslog handler 
	elif log_mode == 'syslog':
		
		sys_log = logging.handlers.SysLogHandler("/dev/log")
		sys_format = logging.Formatter('%(name)s %(levelname)s %(message)s')
		sys_log.setFormatter(sys_format)
		
		logger.addHandler(sys_log)
	
	return logger