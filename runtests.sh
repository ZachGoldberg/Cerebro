#!/bin/bash
export PYTHONPATH=.:..:./src/:$PYTHONPATH
nosetests test/ -v --process-timeout=10 "$@"
