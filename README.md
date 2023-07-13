# pyassistantbot2

## gstasks

### edit
./gstasks.py ls -g ! -ojson|jq '.[]|.uuid' -r|./gstasks.py edit -f- --comment 'test'
