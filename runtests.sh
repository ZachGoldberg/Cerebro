#!/bin/bash
export PYTHONPATH=.:..:$PYTHONPATH
nosetests test/ -v  $@
