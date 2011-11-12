#!/bin/bash
export PYTHONPATH=.:..:./src/:$PYTHONPATH
cd test/
nosetests -sv test_basic.py:BasicTests.$1

