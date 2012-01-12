#!/bin/bash
export PATH=./bin:$PATH
export PYTHONPATH=.:..:./src/:$PYTHONPATH
nosetests test/ -v --process-timeout=10 "$@"
