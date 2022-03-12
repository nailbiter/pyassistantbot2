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
import click
import _common
import sqlite3


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
        return r["uuid"]


class ConvenientCliDatetimeParamType(click.ParamType):
    name = "convenient_cli_datetime"

    def convert(self, value, param, ctx):
        return _common.parse_cmdline_datetime(value, fail_callback=lambda msg: self.fail(msg, param, ctx))


CLI_DATETIME = ConvenientCliDatetimeParamType()


class TagProcessor:
    def __init__(self, coll):
        self._coll = coll
        self._cache = {}
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_all_tags(self):
        return pd.DataFrame(self._coll.find())

    def _get_tag_imputation_record(self, tag):
        return {
            "name": tag,
            "uuid": str(uuid.uuid4()),
        }

    def _fetch_tag(self, **kwargs):
        logger = self._logger
        assert set(kwargs) <= {"uuid", "name"}
        assert sum([v is not None for v in kwargs.values()]) == 1, kwargs
        df = pd.DataFrame(self._coll.find(kwargs))
        assert len(df) <= 1, (tag, df)
        if len(df) == 0:
            assert kwargs["name"] is not None, kwargs
            tag_r = self._get_tag_imputation_record(kwargs["name"])
            logger.warning(f"insert {tag_r}")
            self._coll.insert_one(tag_r)
        else:
            tag_r = df.to_dict(orient="records")[0]
        return tag_r

    def _get_tag_record_or_impute(self, tag=None, uuid=None):
        this_function_name = cast(
            types.FrameType, inspect.currentframe()).f_code.co_name
        logger = logging.getLogger(__name__).getChild(this_function_name)

        if uuid is not None:
            _res = [k for k, v in self._cache.items() if v == uuid]
            assert len(_res) <= 1, (_res, uuid, self._cache)
            if len(_res) == 0:
                _res = [self._fetch_tag(uuid=uuid)["name"]]
            return _res[0]
        if tag is not None:
            if tag in self._cache:
                tag_r = self._cache[tag]
            else:
                tag_r = self._fetch_tag(name=tag)
                self._cache[tag] = tag_r

            return tag_r
        else:
            raise NotImplementedError()

    def __call__(self, tag):
        """
        return tag_uuid
        """
        tag_r = self._get_tag_record_or_impute(tag=tag)
        return tag_r["uuid"]

    def tag_uuid_to_tag_name(self, uuid):
        return self._get_tag_record_or_impute(uuid=uuid)


class UuidCacher:
    def __init__(self, cache_database_filename=".uuid_cache.db"):
        self._cache_database_filename = cache_database_filename
        self._db_name = "uuid_cache"
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_conn(self):
        conn = sqlite3.connect(self._cache_database_filename)
        return conn

    def add(self, uuid, name):
        df = pd.DataFrame(
            [{"uuid": uuid, "datetime": datetime.now().isoformat(), "name": name}])
        conn = self._get_conn()
        df.to_sql(self._db_name, conn, if_exists="append", index=None)
        conn.close()
        self._logger.warning(f"add \"{uuid}\" to cache")

    def get_all(self):
        conn = self._get_conn()
        df = pd.read_sql(f"select * from {self._db_name}", conn)
        conn.close()

        df.datetime = df.datetime.apply(datetime.fromisoformat)
        df = df.sort_values(by="datetime")
        return df
