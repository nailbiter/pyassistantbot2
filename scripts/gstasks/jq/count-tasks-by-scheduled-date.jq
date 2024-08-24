# ./gstasks.py ls -b tomorrow -o json -c uuid -c scheduled_date 
[[.[]|{k:(.scheduled_date/1000)|strftime("%Y-%m-%d (%a)"),u:.uuid}]|group_by(.k)|.[]|{key:.[0].k,value:.|length}]|sort_by(.key)|from_entries
