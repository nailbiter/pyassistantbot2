#!/bin/sh

./ttask.py -r '#remind' -o json > `random-fn.py .json`
cat `random-fn.py -r .json` | jq '.[]|.index'|./ttask.py -l-
