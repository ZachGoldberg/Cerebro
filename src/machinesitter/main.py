#!/usr/bin/python
"""
This is the main file of the machinesitter application.

It's job is to manage multiple tasks on a machine -- starting
and stopping them at the whim of an administrator.
"""

import arg_parser as argparse
import sys

import constraints
import http_monitor
import logmanager
import process_harness
import stats_collector




