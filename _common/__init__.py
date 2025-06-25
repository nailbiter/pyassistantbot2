"""===============================================================================

        FILE: common.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-12-14T10:50:12.918934
    REVISION: ---

==============================================================================="""
import pymongo
from datetime import datetime, timedelta
from pytz import timezone
from bson.codec_options import CodecOptions
from pytz import timezone
import re
import types
from typing import cast
import inspect
from jinja2 import Template
import logging
import subprocess
import os
import logging
import typing
import time
from bson.codec_options import CodecOptions
import uuid
from os import path
import pandas as pd
import sys

TIME_CATS = [
    "sleeping",
    "logistics",
    "reading",
    "rest",
    "useless",
    "coding",
    "parttime",
    "gym",
    "work",
    "social",
    "german",
    "math project",
]

# MONGO_COLL_NAME = "pyassistantbot2"
MONGO_COLL_NAME = "logistics"


def get_sleeping_state(mongo_client):
    """
    return None or (is_no_bother,state)
    """
    mongo_coll = mongo_client[MONGO_COLL_NAME]["alex.sleepingtimes"]
    last_record = mongo_coll.find_one(sort=[("startsleep", pymongo.DESCENDING)])
    if last_record.get("endsleep", None) is not None:
        return None
    else:
        cat = last_record["category"]
        return cat == "sleeping", cat


def to_utc_datetime(date: typing.Optional[datetime] = None, inverse: bool = False):
    if date is None:
        date = datetime.now()
    td = timedelta(hours=_get_current_offset())
    return date - td if not inverse else date + td


def get_remote_mongo_client(mongo_pass):
    return pymongo.MongoClient(
        f"mongodb+srv://nailbiter:{mongo_pass}@cluster0.gaq9o.mongodb.net/logistics?authSource=admin&replicaSet=atlas-1372ty-shard-0&w=majority&readPreference=primary&appname=MongoDB%20Compass&retryWrites=true&ssl=true"
    )


TIMEDELTA_ABBREVIATIONS = {
    ## idea: match with codes in https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    **{s[0].upper(): f"{s}s" for s in ["minute", "hour", "years"]},
    **{s[0].lower(): f"{s}s" for s in ["day", "month"]},
}


def parse_cmdline_datetime(s, fail_callback=None) -> typing.Optional[datetime]:
    try:
        if s is None:
            return None
        elif s == "tomorrow":
            res = datetime.now().date() + timedelta(days=1)
            res = datetime(**{k: getattr(res, k) for k in "year,month,day".split(",")})
            return res
        elif s == "yesterday":
            res = datetime.now().date() - timedelta(days=1)
            res = datetime(**{k: getattr(res, k) for k in "year,month,day".split(",")})
            return res
        elif s == "today":
            res = datetime.now().date()
            res = datetime(**{k: getattr(res, k) for k in "year,month,day".split(",")})
            return res
        elif (m := re.match(r"next (mon|tue|wed|thu|fri|sat|sun)", s)) is not None:
            weekday = "mon|tue|wed|thu|fri|sat|sun".split("|").index(m.group(1))
            res = datetime.now().date() + timedelta(days=1)
            while res.weekday() != weekday:
                res += timedelta(days=1)
            return datetime(**{k: getattr(res, k) for k in "year,month,day".split(",")})
        elif (m := re.match(r"([\+-])(\d+)d", s)) is not None:
            res = datetime.now().date()
            res += (1 if m.group(1) == "+" else -1) * timedelta(days=int(m.group(2)))
            res = datetime(**{k: getattr(res, k) for k in "year,month,day".split(",")})
            return res
        elif (m := re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", s)) is not None:
            return datetime.strptime(s, "%Y-%m-%d %H:%M")
        elif (m := re.match(r"\d{4}-\d{2}-\d{2}", s)) is not None:
            return datetime.strptime(s, "%Y-%m-%d")
        else:
            raise NotImplementedError(f'cannot parse parse_cmdline_datetime "{s}"')
    except Exception:
        if fail_callback is not None:
            fail_callback(f"cannot parse {s}")
        raise


_DEFAULT_TRELLO_PACKAGE_PATH = "/Users/nailbiter/for/forpython/trello"


def run_trello_cmd(cmd, trello_path=None):
    if trello_path is None:
        trello_path = os.environ.get(
            "TRELLO_PACKAGE_PATH", _DEFAULT_TRELLO_PACKAGE_PATH
        )

    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    cmd = Template(
        """
    cd {{trello_path}} {{" && . .envrc " if is_use_envrc}} && ./trello.py --trello_key {{trello_key}} --trello_token {{trello_token}} {{cmd}}
    """
    ).render(
        {
            "trello_path": trello_path,
            "cmd": cmd.strip(),
            "trello_key": os.environ["TRELLO_KEY"],
            "trello_token": os.environ["TRELLO_TOKEN"],
            "is_use_envrc": trello_path == _DEFAULT_TRELLO_PACKAGE_PATH,
        }
    )
    cmd = cmd.strip()
    logger.info(f"cmd: {cmd}")
    ec, out = subprocess.getstatusoutput(cmd)
    assert ec == 0, (cmd, out, ec)
    return out.strip()


class TimerContextManager:
    def __init__(self, name, printer=None):
        self._name = name
        if printer is None:
            printer = logging.warning
        self._printer = printer

    def __enter__(self):
        self._start_time = time.time()
        self._printer(
            f'start block "{self._name}" at {datetime.fromtimestamp(self._start_time)}'
        )

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._end_time = time.time()
        self._printer(
            f'end block "{self._name}" at {datetime.fromtimestamp(self._end_time)}'
        )
        self._printer(
            f"it took {str(timedelta(seconds=self._end_time-self._start_time))}"
        )


def _get_current_offset():
    # code below is adapted from https://stackoverflow.com/a/10854983
    offset = time.timezone if (time.localtime().tm_isdst == 0) else time.altzone
    offset_hour = int(offset / 60 / 60 * -1)
    return offset_hour


def get_coll(mongo_pass, collection_name="alex.time", apply_options=False):
    if apply_options:
        logging.warning(f"`apply_options=True` is deprecated")
    client = get_remote_mongo_client(mongo_pass)
    coll = client.logistics[collection_name]
    if apply_options:
        coll = coll.with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=timezone("Asia/Tokyo"))
        )
    return coll


def spl(s, sep=None):
    if sep is None:
        s, sep = s[:-1], s[-1]
    return s.split(sep)


def split_long_text(text, max_len, line_sep="\n"):
    res = []
    accum, buf = 0, []
    for line in text.split(line_sep):
        assert len(line) <= max_len, f'len("{line}")={len(line)}>{max_len}'
        if accum + len(line) > max_len:
            res.append(line_sep.join(buf))
            accum, buf = len(line), [line]
        else:
            accum += len(line) + len(line_sep)
            buf.append(line)
    if len(buf) > 0:
        res.append(line_sep.join(buf))
    return res


def _align_dt(dt, td, align_logic="left"):
    assert align_logic in ["left"]
    ts = td.total_seconds()
    return datetime.fromtimestamp((dt.timestamp() // ts) * ts)


def consecutive_periods(dates, td, is_normalize=True):
    if is_normalize:
        dates = sorted({_align_dt(dt, td) for dt in dates})
    res = [{"start": dates[0]}]
    res[-1]["anchor"] = res[-1]["start"]
    while True:
        if res[-1]["anchor"] + td in dates:
            #            print(res)
            res[-1]["anchor"] += td
        #            print(res)
        # exit(1)
        else:
            res[-1]["end"] = res[-1].pop("anchor")
            # print((res[-1], dates[-1], len(dates), min(dates),len(res)))
            if res[-1]["end"] >= dates[-1]:
                break
            else:
                res.append({"start": res[-1]["end"] + td})
                while res[-1]["start"] not in dates:
                    res[-1]["start"] += td
                res[-1]["anchor"] = res[-1]["start"]
    return res


def fill_gaps(dates, td, is_normalize=True):
    """
    return sorted values
    """
    #    click.echo(dates)
    if is_normalize:
        dates = sorted({_align_dt(dt, td) for dt in dates})
    full_dates = [dates[0]]
    while full_dates[-1] < dates[-1]:
        full_dates.append(full_dates[-1] + td)
    assert set(full_dates) >= set(dates), set(dates) - set(full_dates)
    return sorted(set(full_dates) - set(dates))


def get_random_fn(
    ext,
    tmp_dir="/tmp",
):
    assert ext.startswith("."), ext
    return path.join(tmp_dir, f"{uuid.uuid4()}{ext}")


def get_configured_logger(
    name: str,
    level="DEBUG",
    format_string: typing.Optional[str] = " - ".join(
        [
            f"%({n})s"
            for n in [
                "name",
                "asctime",
                "levelname",
                "message",
            ]
        ]
    ),
) -> logging.Logger:
    "move to altp"
    app_logger = logging.getLogger(name)
    app_logger.setLevel(getattr(logging, level))
    app_console_handler = logging.StreamHandler(sys.stderr)
    if format_string is not None:
        formatter = logging.Formatter(
            # "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            format_string
        )
        app_console_handler.setFormatter(formatter)
    app_logger.addHandler(app_console_handler)
    return app_logger
