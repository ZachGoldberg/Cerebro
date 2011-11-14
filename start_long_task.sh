#!/bin/bash
./tasksitter --command "date; sleep 5" --restart --ensure-alive --http-monitoring --http-monitoring-port=$1 --stdout-location=/tmp/tasks --stderr-location=/tmp/tasks
