#!/bin/sh

echo 'FIXME: may be buggy'

now=`date "+%Y-%m-%dT%H:%M:%S"`
echo $now

./ttask.py -r '#remind' -o json > `random-fn.py .json`
cat `random-fn.py -r .json`|jq '.[]|.index'|./ttask.py -l-
cat `random-fn.py -r .json`|jq "[.[]|{name:.content,label:{mongo_id:.[\"_id\"],date:.date,move_date:\"$now\"},when:\"EVENING\"}]"> `random-fn.py .json`
./gstasks.py import-file -f `random-fn.py -r .json` -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind
