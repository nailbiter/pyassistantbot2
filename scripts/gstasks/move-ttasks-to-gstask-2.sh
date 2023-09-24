#!/bin/sh

now=`date "+%Y-%m-%dT%H:%M:%S"`

cat `random-fn.py -r .json` | jq "[.[]|{name:.content,label:{mongo_id:.[\"_id\"],date:.date,move_date:\"$now\"},when:\"EVENING\"}]"|./gstasks.py import-file -f-  -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind #error here

echo $now

#ls
./gstasks.py ls -c uuid -g $GSTASKS_MISC_TASKS_TAG -g remind -g ttask -o json -c label -c name 2>/dev/null|jq ".[]|select(.label.move_date==\"$now\")|[.name,.uuid]|@tsv" -r|tee `random-fn.py .tsv`
echo  `random-fn.py -r .tsv`
