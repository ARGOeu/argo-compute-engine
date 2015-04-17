#!/usr/bin/env python

from subprocess import check_call, CalledProcessError
from os import path
from sys import exit


def run_cmd(cmd_args, log):
    # executable path is the first argument
    exec_path = cmd_args[0]
    exec_name = path.basename(cmd_args[0])

    try:
        check_call(cmd_args)
    except CalledProcessError:
        log.error("Error while executing %s", exec_name)
        exit(1)
    except OSError:
        log.error("Could not locate %s at path: %s", exec_name, exec_path)
        exit(1)
