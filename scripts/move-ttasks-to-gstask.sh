#!/bin/sh

# # Tue Aug 29 17:14:29 2023
# # Tue Aug 29 17:14:13 2023
# cat (random-fn.py -r .json)|jq '[.[]|{name:.content,label:{mongo_id:.["_id"],date:.date}}]'|./gstasks.py import-file -f- -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind --create-new-tag --dry-run
# # Tue Aug 29 17:12:33 2023
# cat (random-fn.py -r .json)|jq '.[0]'
# # Tue Aug 29 17:11:34 2023
# cat (random-fn.py -r .json)|jq '[.[]|{name:.content,label:{mongo_id:.["_id"],date:.date}}]'|./gstasks.py import-file -f- -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind --create-new-tag -s today --dry-run
# # Tue Aug 29 17:02:25 2023
# cat (random-fn.py -r .json)|jq '.[]|{name:.content,label:{mongo_id:.["_id"],date:.date}}'|./gstasks.py import-file -f- -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind --create-new-tag
# # Tue Aug 29 17:02:00 2023
# cat (random-fn.py -r .json)|jq '.[]|{name:.content,label:{mongo_id:.["_id"],date:.date}}'
# # Tue Aug 29 17:01:39 2023
# cat (random-fn.py -r .json)|jq '{name:.content,label:{mongo_id:.["_id"],date:.date}}'|./gstasks.py import-file -f- -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind --create-new-tag
# # Tue Aug 29 17:00:53 2023
# cat (random-fn.py -r .json)|jq '.'|./gstasks.py import-file -f- -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind --create-new-tag
# # Tue Aug 29 17:00:46 2023
# cat (random-fn.py -r .json)|jq '.'|./gstasks.py import-file -f- -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind
# # Tue Aug 29 16:53:29 2023
# cat (random-fn.py -r .json)|jq
# # Tue Aug 29 16:53:21 2023
# cat (random-fn.py -r .json)
# # Tue Aug 29 16:52:01 2023
# # Tue Aug 29 16:51:29 2023
# ls scripts
# # Tue Aug 29 16:50:32 2023
# cat (random-fn.py -r .json)|jq '.[]|.index'|./ttask.py -l- --dry-run
# # Tue Aug 29 16:50:21 2023
# cat (random-fn.py -r .json)|jq '.[]|.index'|./ttask.py -l --dry-run
# # Tue Aug 29 16:49:57 2023
# cat (random-fn.py -r .json)|jq '.[]|.index'
# # Tue Aug 29 16:49:44 2023

# main
./ttask.py -r '#remind' -o json > `random-fn.py .json`
cat `random-fn.py -r .json`|jq '.[]|.index'|./ttask.py -l-
# cat (random-fn.py -r .json)|jq '[.[]|{name:.content,label:{mongo_id:.["_id"],date:.date}}]'|./gstasks.py import-file -f- -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind --create-new-tag
cat `random-fn.py -r .json`|jq '[.[]|{name:.content,label:{mongo_id:.["_id"],date:.date}}]'|./gstasks.py import-file -f- -g $GSTASKS_MISC_TASKS_TAG -g ttask -g remind
