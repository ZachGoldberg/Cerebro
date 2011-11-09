#!/bin/bash
export PYTHONPATH=.:..:./src/:$PYTHONPATH
nosetests test/test_http_monitoring.py -v --process-timeout=10 $@
