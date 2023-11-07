#!/bin/sh

./ttask.py -o json > `random-fn.py .json`
cat `random-fn.py -r .json` | jq '.[]|.index'|./ttask.py -l-
