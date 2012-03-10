#!/bin/bash
export PYTHONPATH=.:..:./src/:$PYTHONPATH
./bin/nosetests test/test_http_monitoring.py -v --process-timeout=10 "$@"
