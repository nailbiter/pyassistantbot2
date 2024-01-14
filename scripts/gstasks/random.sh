#!/bin/sh
./gstasks.py ls -o json -c name -c status -c scheduled_date -c uuid -b today|jq '.[]|[.name,.uuid[:7],.status]|@tsv' -r |shuf -n10
