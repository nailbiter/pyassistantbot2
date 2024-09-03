#!/bin/sh

TMP_FN=/tmp/gstask-scoop.txt

./gstasks.py ls -b today -o json -c uuid -c name|jq '.[]|[.uuid,.name]|@tsv' -r|grep 'pull 10'|head -n 2|awk -F'\t' '{print $1}' > $TMP_FN

#jira-cli.py -d api issue add -p 10008 -s pull -t 10020 -d pull

#|./gstasks.py edit -f- -tDONE
