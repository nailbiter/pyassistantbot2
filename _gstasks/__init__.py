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

# pip3 install python-dateutil
import hashlib
import more_itertools
import inspect
import json
from _gstasks.labels_types import LABELS_TYPES
from _gstasks.task_list import TaskList
from _gstasks.base import _format_url, make_mongo_friendly
import json5
import logging
import functools
import numpy as np
import operator
import os
from os import path
import re
import sqlite3
import itertools
import string
import subprocess
import sys
import types
import uuid
from datetime import datetime, timedelta
from typing import cast
import typing
import click
import pandas as pd
from jinja2 import Template
from pymongo import MongoClient
import pymongo.collection
from bson.codec_options import CodecOptions
from dateutil.relativedelta import relativedelta
import pytz
import _common


_LOCAL_TZ_NAME = "Asia/Tokyo"


def _parse_date(s):
    if not s or pd.isna(s):
        return None
    else:
        m = re.match(
            r"(?P<year>\d+)/(?P<month>\d+)/(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)(\:(?P<second>\d+))?",
            s,
        )
        assert m is not None, s
        return datetime(
            **{
                k: int(m.group(k))
                for k in "year,month,day,hour,minute,second".split(",")
                if m.group(k) is not None
            }
        )


_COLUMNS = "name,URL,scheduled_date,status,when,due,uuid".split(",")

TEMPLATE_DIR_DEFAULT = path.join(path.dirname(__file__), "templates")
UUID_CACHE_DB_DEFAULT = path.abspath(
    path.join(path.dirname(__file__), "../.uuid_cache.db")
)


def setup_ctx_obj(
    ctx,
    mongo_url: str,
    list_id: str,
    labels_types_json5: typing.Optional[str] = None,
    uuid_cache_db: str = UUID_CACHE_DB_DEFAULT,
    template_dir: str = TEMPLATE_DIR_DEFAULT,
) -> None:
    # (['task_list', 'list_id', 'uuid_cache_db', 'template_dir']
    ctx.obj["task_list"] = TaskList(
        mongo_url=mongo_url, database_name="gstasks", collection_name="tasks"
    )

    labels_types = {}
    if labels_types_json5 is not None:
        with open(labels_types_json5) as f:
            labels_types = json5.load(f)
    labels_types = {k: LABELS_TYPES[v] for k, v in labels_types.items()}
    ctx.obj["labels_types"] = labels_types

    kwargs = dict(
        list_id=list_id, uuid_cache_db=uuid_cache_db, template_dir=template_dir
    )
    for k, v in kwargs.items():
        ctx.obj[k] = v


class ConvenientCliDatetimeParamType(click.ParamType):
    name = "convenient_cli_datetime"

    def convert(self, value, param, ctx):
        return _common.parse_cmdline_datetime(
            value, fail_callback=lambda msg: self.fail(msg, param, ctx)
        )


CLI_DATETIME = ConvenientCliDatetimeParamType()


class ConvenientCliTimeParamType(click.ParamType):
    name = "convenient_cli_time"

    def __init__(self, now=datetime.now()):
        self._now = now

    def convert(self, value, param, ctx):
        # return _common.parse_cmdline_datetime(
        #     value, fail_callback=lambda msg: self.fail(msg, param, ctx)
        # )
        if (
            m := re.match(
                r"\+([\d]+)([" + "".join(_common.TIMEDELTA_ABBREVIATIONS) + "])$", value
            )
        ) is not None:
            res = self._now + relativedelta(
                **{_common.TIMEDELTA_ABBREVIATIONS[m.group(2)]: int(m.group(1))}
            )
        elif (m := re.match(r"(\d{2}):(\d{2})$", value)) is not None:
            res = self._now.replace(
                **{k: int(m.group(i + 1)) for i, k in enumerate(["hour", "minute"])},
            )
        else:
            res = pd.to_datetime(value, errors="coerce")
            if pd.isna(res):
                self.fail(f'cannot parse "{value}"', param, ctx)

        return date_to_grid(res)


def date_to_grid(dt: datetime, grid_hours: bool = False) -> datetime:
    kw = dict(second=0, microsecond=0)
    if grid_hours:
        kw["hour"] = 0
        kw["minute"] = 0
    return dt.replace(**kw)


CLI_TIME = ConvenientCliTimeParamType


class TagProcessor:
    def __init__(self, coll, create_new_tag=True, flag_name=None):
        self._coll = coll
        self._cache = {}
        self._logger = logging.getLogger(self.__class__.__name__)
        self._create_new_tag = create_new_tag
        self._flag_name = flag_name

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
            msg = f'cannot create new tag "{kwargs}"'
            if self._flag_name is not None:
                msg = f"{msg} (use flag `{self._flag_name}`)"
            assert self._create_new_tag, msg
            assert kwargs["name"] is not None, kwargs
            tag_r = self._get_tag_imputation_record(kwargs["name"])
            logger.warning(f"insert {tag_r}")
            self._coll.insert_one(tag_r)
        else:
            tag_r = df.to_dict(orient="records")[0]
        return tag_r

    def _get_tag_record_or_impute(self, tag=None, uuid=None):
        this_function_name = cast(
            types.FrameType, inspect.currentframe()
        ).f_code.co_name
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

    @functools.cache
    def tag_uuid_to_tag_name(self, uuid):
        return self._get_tag_record_or_impute(uuid=uuid)

    def remove_tag_by_uuid(self, uuid):
        return self._coll.delete_one({"uuid": uuid})


class UuidCacher:
    def __init__(self, cache_database_filename):
        self._cache_database_filename = cache_database_filename
        self._db_name = "uuid_cache"
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_conn(self):
        conn = sqlite3.connect(self._cache_database_filename)
        return conn

    def add(self, uuid, name):
        df = pd.DataFrame(
            [{"uuid": uuid, "datetime": datetime.now().isoformat(), "name": name}]
        )
        conn = self._get_conn()
        df.to_sql(self._db_name, conn, if_exists="append", index=None)
        conn.close()
        self._logger.warning(f'add "{uuid}" to cache')

    def get_all(self):
        conn = self._get_conn()
        df = pd.read_sql(f"select * from {self._db_name}", conn)
        conn.close()

        df.datetime = df.datetime.apply(datetime.fromisoformat)
        df = df.sort_values(by="datetime")
        return df


class StringContractor:
    "TODO: move to `alex_leontiev_toolbox_python`"

    def __init__(self, maxlen=20, ellipsis_symbol="..."):
        assert maxlen > len(ellipsis_symbol)
        self._maxlen = maxlen
        self._ellipsis_symbol = ellipsis_symbol

    def __call__(self, s):
        if len(s) > self._maxlen:
            s = s[: self._maxlen - len(self._ellipsis_symbol)] + self._ellipsis_symbol
        return s


CLICK_DEFAULT_VALUES = {
    "ls": {
        "name_length_limit": 50,
        "filter_out_states": json.dumps(["DONE", "FAILED"]),
        "smart_columns": [],
        "un_scheduled": False,
        "tags": tuple(),
        "when": tuple(),
        "sort_order": tuple(),
    },
    "mark": {"mark": "engage"},
    "relations": {
        "relations_config_file": path.join(
            path.dirname(__file__), "..", ".gstasks_relations.json5"
        )
    },
}


def ssj(s: str) -> str:
    "Strip Split Join"
    return " ".join(s.strip().split())


def dynamic_wait(
    check_interval_minutes: int, now: typing.Optional[datetime] = None
) -> (datetime, timedelta):
    if now is None:
        now = datetime.now()
    now_min = datetime.timestamp(now) / 60
    wait_min = check_interval_minutes * np.ceil(now_min / check_interval_minutes)
    wait_dt = datetime.fromtimestamp(60 * wait_min)
    td = wait_dt - now
    return wait_dt, td


def cmdline_keys_to_sort_kwargs(sort_order: tuple) -> dict:
    kwargs = dict(
        by=[k for k, _ in sort_order],
        ascending=[(a == "asc") for _, a in sort_order],
    )
    return kwargs


def _check_pid(pid):
    """
    Check For the existence of a unix pid.
    taken from https://stackoverflow.com/a/568285
    """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def is_sweep_daemon_running(obj: dict) -> (bool, dict):
    fn = obj["sweep_daemon_pid_file"]
    is_daemon_running, rest = False, {}
    if path.isfile(fn):
        with open(fn) as f:
            rest = json.load(f)
        pid = rest["pid"]
        is_daemon_running = _check_pid(pid)
    res = is_daemon_running, rest
    # logging.warning(res)
    return res


def dump_daemon_pid(is_sweep_daemon_pid: bool, sweep_daemon_pid_file: str, **_):
    # logging.warning(
    #     dict(
    #         is_sweep_daemon_pid=is_sweep_daemon_pid,
    #         sweep_daemon_pid_file=sweep_daemon_pid_file,
    #     )
    # )

    if is_sweep_daemon_pid:
        fn = sweep_daemon_pid_file
        # logging.warning(f"saving pid to `{fn}`")
        with open(fn, "w") as f:
            json.dump(
                {"pid": os.getpid(), "timestamp_iso": datetime.now().isoformat()}, f
            )


def str_or_envvar(s: str, envvar_prefix: str = "$"):
    if s.startswith(envvar_prefix):
        return os.environ[s[len(envvar_prefix) :]]
    else:
        return s


def get_last_engaged_task_uuid(task_list, mark="engage") -> typing.Optional[str]:
    l = list(task_list.get_coll("engage").find({"mark": mark}).sort("dt", -1).limit(1))
    if len(l) == 0:
        return None
    else:
        return l[0]["task_uuid"]


class GstaskUuid(click.ParamType):
    name = "gstask_uuid"

    def convert(self, uuid_text: str, param, ctx):
        task_list = ctx.obj["task_list"]

        r, _ = task_list.get_task(uuid_text=uuid_text)
        uuid_text = r["uuid"]

        # return _common.parse_cmdline_datetime(
        #     value, fail_callback=lambda msg: self.fail(msg, param, ctx)
        # )
        return uuid_text


GSTASK_UUID = GstaskUuid()


def smart_processor(df: pd.DataFrame, processor: str) -> pd.Series:
    if (m := re.match(r"(.*)\[:(\d+)\]$", processor)) is not None:
        res = df[m.group(1)].str[: int(m.group(2))]
        # logging.warning(res)
        return res
    else:
        raise NotImplementedError((processor,))


def preprocess_stopwatch_slice(df: pd.DataFrame) -> list[dict]:
    assert set(df["action"]) <= {"stop", "start"}
    now = datetime.now()
    rs = df.sort_values(by="now").to_dict(orient="records")

    rs = [
        dict(
            action=action,
            now=min(map(operator.itemgetter("now"), group))
            if action == "start"
            else max(map(operator.itemgetter("now"), group)),
        )
        for action, group in itertools.groupby(rs, key=operator.itemgetter("action"))
    ]

    while (rs[0]["action"] == "stop") and (len(rs) > 0):
        rs.pop(0)
    return rs


def process_stopwatch_slice(df: pd.DataFrame) -> dict:
    """
    @return (is_running,elapsed)
    """
    # assert len(df)>0
    rs = preprocess_stopwatch_slice(df)

    if len(rs) == 0:
        return dict(is_running=False, elapsed=timedelta(0))
    elif len(rs) == 1:
        (r,) = rs
        logging.warning(r)
        return dict(is_running=True, elapsed=now - r["now"])
    elif len(rs) % 2 == 0:
        return dict(
            is_running=False,
            elapsed=sum(
                [b["now"] - a["now"] for a, b in more_itertools.batched(rs, 2)],
                start=timedelta(0),
            ),
        )
    elif len(rs) % 2 == 1:
        *head, tail = rs
        return dict(
            is_running=True,
            elapsed=sum(
                [b["now"] - a["now"] for a, b in more_itertools.batched(head, 2)],
                start=timedelta(0),
            )
            + (now - tail["now"]),
        )
        pass
    else:
        raise NotImplementedError(rs)


@functools.cache
def next_work_day(dt: datetime, inc: int = 1) -> datetime:
    """
    FIXME: current runtime O(n) is pathetic, make it O(1)
    """
    if inc < 0:
        raise NotImplementedError("FIXME: enable negative increment")
    while inc > 0:
        dt += timedelta(days=1)
        if dt.isoweekday() in list(range(1, 6)):
            inc -= 1
    return dt


def real_worktime_add(
    coll: pymongo.collection.Collection,
    task_uuid: str,
    duration_sec: int,
    now: typing.Optional[datetime] = None,
    comment: typing.Optional[str] = None,
):
    return coll.insert_one(
        {
            "task_uuid": task_uuid,
            "uuid": str(uuid.uuid4()),
            "duration_sec": duration_sec,
            "now": datetime.now() if now is None else now,
            "version": "v2",
            "comment": comment,
        }
    )


def real_worktime_ls(
    coll: pymongo.collection.Collection,
    task_uuid: str,
) -> pd.DataFrame:
    df = pd.DataFrame(coll.find({"task_uuid": task_uuid}))
    if len(df) > 0:
        df.drop(columns=["task_uuid"], inplace=True)
        df.set_index("uuid", inplace=True)
    return df


def urllize_df(
    df: pd.DataFrame, cns: list[str], url_root: str = "http://127.0.0.1:5000"
) -> pd.DataFrame:
    if len(df) > 0:
        for cn in cns:
            df[cn] = df[cn].apply(lambda u: f'<a href="{url_root}/lso/{u}">{u}</a>')
    return df


def real_rolling_log_add(
    task_uuid: str,
    coll: pymongo.collection.Collection,
    url: str,
    omit_url_check: bool = False,
    date_time: typing.Optional[datetime] = None,
    comment: typing.Optional[str] = None,
):
    if not omit_url_check:
        allowed_url_prefixes = ["http://", "https://"]
        assert np.any([url.startswith(k) for k in allowed_url_prefixes]), (
            allowed_url_prefixes,
            url,
        )

    r = dict(
        uuid=str(uuid.uuid4()),
        task_uuid=task_uuid,
        date_time=datetime.now() if date_time is None else date_time,
        url=url,
        comment=comment,
    )
    logging.warning(r)
    res = coll.insert_one(r)
    return res
