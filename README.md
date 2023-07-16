# pyassistantbot2

## gstasks

### edit
./gstasks.py ls -g ! -ojson|jq '.[]|.uuid' -r|./gstasks.py edit -f- --comment 'test'

### terminology

#### marks, tags, labels, flabels

* **mark** -- can only be at one (or zero) tasks at any given time
* **tag** -- for every task, tag can be either "on" or "off"
* **label** -- for every task, label can be set to arbitrary string (may change in future) value; think of a Python dictionary associated
with every task
* **flabel** -- (=fuzzy labels)
