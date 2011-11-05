#!/bin/bash
export PYTHONPATH=.:..:$PYTHONPATH
nosetests test/ -v --process-timeout=10 $@
