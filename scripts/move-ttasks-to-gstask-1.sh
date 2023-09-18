#!/bin/sh

echo 'FIXME: may be buggy'

./ttask.py -r '#remind' -o json > `random-fn.py .json`
cat `random-fn.py -r .json` | jq '.[]|.index'|./ttask.py -l-
