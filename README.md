# pyassistantbot2

## pyassistantbot2

### build 

```sh
make
docker-compose build
```

### test

### run

```sh
#
```

## deploy to Cloud Run

```
time gcloud run deploy pyas2-habits \
  --source . \
  --region us-east1 --no-allow-unauthenticated;
```

## gstasks

### edit
./gstasks.py ls -g ! -ojson|jq '.[]|.uuid' -r|./gstasks.py edit -f- --comment 'test'

### helpful `.envrc` exceprts

```
export GSTASKS_EDIT_POST_HOOK="./gstasks.py ls -o html"
```

### how to run server

```sh
flask --app gstasks-flask run --debug
```

### terminology

#### marks, tags, labels, flabels

* **mark** -- can only be at one (or zero) tasks at any given time
* **tag** -- for every task, tag can be either "on" or "off"
* **label** -- for every task, label can be set to arbitrary string (may change in future) value; think of a Python dictionary associated
with every task
* **flabel** -- (=fuzzy labels) same as label but instead of *dict* we have key-value pairs (meaning that the keys may duplicate)
* **relation** -- relation between tasks (e.g. `blocks`, `included in`); directed binary
