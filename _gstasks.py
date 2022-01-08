"""===============================================================================

        FILE: forhabits/kostil/_gstasks.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION:

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION:
     VERSION: ---
     CREATED: 2021-09-04T17:20:04.948789
    REVISION: ---

==============================================================================="""

import pandas as pd
from datetime import datetime, timedelta
import re
import string
import uuid
import inspect
import types
from typing import cast
import logging
import json
from jinja2 import Template
import subprocess
import hashlib
import sys
from pymongo import MongoClient


def _parse_date(s):
    if not s or pd.isna(s):
        return None
    else:
        m = re.match(
            r"(?P<year>\d+)/(?P<month>\d+)/(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)(\:(?P<second>\d+))?", s)
        assert m is not None, s
        return datetime(**{k: int(m.group(k)) for k in "year,month,day,hour,minute,second".split(",") if m.group(k) is not None})


def _format_url(url):
    if not url:
        return ""
    elif url.startswith("https://trello.com/c/"):
        return "T"
    else:
        return "U"


_COLUMNS = "name,URL,scheduled_date,status,when,due,uuid".split(",")


class TaskList:
    def __init__(self, mongo_url, database_name, collection_name):
        self._mongo_client = MongoClient(mongo_url)
        self._database_name = database_name
        self._collection_name = collection_name
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_all_tasks(self, post_processing=True):
        df = pd.DataFrame(self.get_coll().find())
#        df = df.sort_values(by=["_insertion_date", "_id"])
        df = df.drop(columns=[x for x in list(df) if x.startswith("_")])
        if post_processing:
            df.insert(1, "U", df.pop("URL").apply(_format_url))

        return df

    def _log(self, **kwargs):
        r = kwargs
        r["timestamp"] = datetime.now()
        self._logger.info(r)
        self.get_coll(collection_name="actions").insert_one(r)

    def get_task(self, uuid_text=None, index=None):
        assert sum([x is not None for x in [index, uuid_text]]) == 1
        df = self.get_all_tasks(post_processing=False)
        if index is not None:
            r = df.to_dict(orient="records")[index]
        elif uuid_text is not None:
            slice_ = [(i, r) for i, r in enumerate(df.to_dict(
                orient="records")) if r["uuid"].startswith(uuid_text)]
            assert len(slice_) == 1, (uuid_text, slice_)
            index, r = slice_[0]
        return r, index

    def get_coll(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        return self._mongo_client[self._database_name][collection_name]

    def insert_or_replace_record(self, r, index=None):
        action = "inserting" if index is None else "replacing"
        print(f"{action} {r}", file=sys.stderr)
        if index is None:
            index = len(self.get_all_tasks())

        if "uuid" not in r:
            r["uuid"] = str(uuid.uuid4())
        # FIXME: separate `insertion_date` and `last_update_date`
        r["_insertion_date"] = datetime.now()
        # FIXME: why this happens?
        for k in ["due", "scheduled_date"]:
            if pd.isna(r[k]):
                r[k] = None
        # exit(1)
        log_kwargs = {}
        if action == "replacing":
            log_kwargs["previous_r"] = self.get_coll().find_one(
                {"uuid": r["uuid"]})
        self._log(action=action, r=r, **log_kwargs)
        self.get_coll().replace_one(
            filter={"uuid": r["uuid"]}, replacement=r, upsert=True)
        print(r["uuid"])
