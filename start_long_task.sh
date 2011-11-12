#!/bin/bash
./tasksitter --command "sleep 5" --restart --ensure-alive --http-monitoring --http-monitoring-port=$1
