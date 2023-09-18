#!/bin/sh

echo 'FIXME: may be buggy'

now=`date "+%Y-%m-%dT%H:%M:%S"`

./ttask.py -r '#remind' -o json > `random-fn.py .json`
cat `random-fn.py -r .json` | jq '.[]|.index'|./ttask.py -l-
cat `random-fn.py -r .json` | jq "[.[]|{name:.content,label:{mongo_id:.[\"_id\"],date:.date,move_date:\"$now\"},when:\"EVENING\"}]"> `random-fn.py .json`
./gstasks.py import-file -f `random-fn.py -r .json` -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind #error here

echo $now

#ls
#./gstasks.py ls -c uuid -g $GSTASKS_MISC_TASKS_TAG -g remind -g ttask -o json -c label -c name 2>/dev/null|jq '.[]|select(.label.move_date=="2023-09-17T22:56:50")|[.name,.uuid]|@tsv' -r
