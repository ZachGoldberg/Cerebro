#!/bin/bash
export PATH=../bin:$PATH
export PYTHONPATH=.:..:../src/:$PYTHONPATH
nosetests . -v --process-timeout=10 "$@"
