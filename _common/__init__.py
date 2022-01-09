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
import re
import types
from typing import cast
import inspect
from jinja2 import Template
import logging
import subprocess
import os

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

#MONGO_COLL_NAME = "pyassistantbot2"
MONGO_COLL_NAME = "logistics"


def get_sleeping_state(mongo_client):
    """
    return None or (is_no_bother,state)
    """
    mongo_coll = mongo_client[MONGO_COLL_NAME]["alex.sleepingtimes"]
    last_record = mongo_coll.find_one(
        sort=[("startsleep", pymongo.DESCENDING)])
    if last_record.get("endsleep", None) is not None:
        return None
    else:
        cat = last_record["category"]
        return cat == "sleeping", cat


def to_utc_datetime(date=None, inverse=False):
    if date is None:
        date = datetime.now()
    td = timedelta(hours=9)
    return date-td if not inverse else date+td


def get_remote_mongo_client(mongo_pass):
    return pymongo.MongoClient(
        f"mongodb+srv://nailbiter:{mongo_pass}@cluster0.gaq9o.mongodb.net/logistics?authSource=admin&replicaSet=atlas-1372ty-shard-0&w=majority&readPreference=primary&appname=MongoDB%20Compass&retryWrites=true&ssl=true")


def get_coll(mongo_pass, collection_name):
    client = get_remote_mongo_client(mongo_pass)
    coll = client.logistics[collection_name].with_options(
        codec_options=CodecOptions(tz_aware=True, tzinfo=timezone('Asia/Tokyo')))
    return coll


def parse_cmdline_date(s):
    if s is None:
        return None
    elif s == "tomorrow":
        res = datetime.now().date()+timedelta(days=1)
        res = datetime(**{k: getattr(res, k)
                       for k in "year,month,day".split(",")})
        return res
    elif s == "yesterday":
        res = datetime.now().date()-timedelta(days=1)
        res = datetime(**{k: getattr(res, k)
                       for k in "year,month,day".split(",")})
        return res
    elif s == "today":
        res = datetime.now().date()
        res = datetime(**{k: getattr(res, k)
                       for k in "year,month,day".split(",")})
        return res
    elif (m := re.match(r"next (mon|tue|wed|thu|fri|sat|sun)", s)) is not None:
        weekday = "mon|tue|wed|thu|fri|sat|sun".split("|").index(m.group(1))
        res = datetime.now().date()
        while res.weekday() != weekday:
            res += timedelta(days=1)
        return datetime(**{k: getattr(res, k)
                           for k in "year,month,day".split(",")})
    elif re.match(r"[\+-](\d+)d", s) is not None:
        m = re.match(r"([\+-])(\d+)d", s)
        res = datetime.now().date()
        res += (1 if m.group(1) == "+" else -1)*timedelta(days=int(m.group(2)))
        res = datetime(**{k: getattr(res, k)
                       for k in "year,month,day".split(",")})
        return res
    else:
        return datetime.strptime(s, "%Y-%m-%d")


_DEFAULT_TRELLO_PACKAGE_PATH = "/Users/nailbiter/for/forpython/trello"


def run_trello_cmd(cmd, trello_path=None):
    if trello_path is None:
        trello_path = os.environ.get(
            "TRELLO_PACKAGE_PATH", _DEFAULT_TRELLO_PACKAGE_PATH)

    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(
        types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    cmd = Template("""
    cd {{trello_path}} {{" && . .envrc " if is_use_envrc}} && ./trello.py --trello_key {{trello_key}} --trello_token {{trello_token}} {{cmd}}
    """).render({
        "trello_path": trello_path,
        "cmd": cmd.strip(),
        "trello_key": os.environ["TRELLO_KEY"],
        "trello_token": os.environ["TRELLO_TOKEN"],
        "is_use_envrc": trello_path == _DEFAULT_TRELLO_PACKAGE_PATH,
    })
    cmd = cmd.strip()
    logger.info(f"cmd: {cmd}")
    ec, out = subprocess.getstatusoutput(cmd)
    assert ec == 0, (cmd, out, ec)
    return out.strip()
