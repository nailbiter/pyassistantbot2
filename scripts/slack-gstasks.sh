#!/bin/sh

FILE=/tmp/tasks.txt
. ./.envrc

rm -rf $FILE
echo '```'>>$FILE
./gstasks.py ls -w EVENING -b today --tag ! -o str>>/tmp/tasks.txt
echo '```'>>$FILE
cat /tmp/tasks.txt | jq -Rs '{text:.}'|curl -X POST -H 'Content-type: application/json' --data @- $SLACK_WEBHOOK

