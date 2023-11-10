#./gstasks.py ls -u -o json -c name -c uuid -c due -c scheduled_date|jq -f scripts/gstasks/jq/filter-dues.jq -r
.[]|select(.due!=null)|.+{d:(.due/1000)|todate,sd:(.scheduled_date/1000)|todate}|[((.uuid|split("-"))[0]),.sd,.d,.name]|@tsv
