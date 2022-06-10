#!/bin/sh

FILE=/tmp/tasks.txt
UUID_FILE=/tmp/task_uuids.txt
. ./.envrc

rm -rf $FILE
echo '```'>>$FILE
./gstasks.py ls -w $1 -b today --tag ! -o str>>$FILE
./gstasks.py ls -w $1 -b today --tag ! -o json|/snap/bin/jq -r '.[]|.uuid'>$UUID_FILE
cat $UUID_FILE
wc $UUID_FILE
echo '```'>>$FILE

cat $UUID_FILE|parallel --jobs 1 ./gstasks.py edit -u {} -g!
./scripts/insert-into-db.py -f $UUID_FILE -n uuid -d "gstasks" -c 'regular_checkup' -k when $1 -t datetime
cat $FILE | /snap/bin/jq -Rs '{text:.}'|curl -X POST -H 'Content-type: application/json' --data @- $SLACK_WEBHOOK
