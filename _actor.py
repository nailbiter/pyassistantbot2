"""===============================================================================

        FILE: _actor.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-12-15T13:30:12.967895
    REVISION: ---

==============================================================================="""
import _common
import re
from datetime import datetime, timedelta
import subprocess
import pymongo
import _common
from _common import spl
import _common.simple_math_eval
import heartbeat_time
import os
import logging
import random
import string


def add_money(text, send_message_cb=None, mongo_client=None):
    amount, *other = re.split(r" +", text)
    amount = _common.simple_math_eval.simple_math_eval(amount)
    assert amount != 0, "amount==0"
    tags = set()
    date = datetime.now()
    category = None
    for i, x in enumerate(other):
        if x.startswith("#"):
            x = x[1:]
            if x in ["food", "fun"]:
                category = x
            else:
                tags.add(x)
        elif x.startswith("%"):
            x = x[1:]
            if len(x) == 12:
                date = datetime.strptime(x, "%Y%m%d%H%M")
            elif len(x) == 6:
                date = datetime.strptime(x, "%d%H%M")
                now = datetime.now()
                date = date.replace(**{k: getattr(now, k)
                                    for k in spl("year,month,")})
        else:
            break
    i += 1
    comment = " ".join(other[i:])
    assert category is not None, "no category"
    mongo_client[_common.MONGO_COLL_NAME]["alex.money"].insert_one({
        "date": _common.to_utc_datetime(date),
        "comment": comment,
        "tags": sorted(list(tags)),
        "category": category,
        "amount": amount,
    })
    send_message_cb(
        f"added amount {amount} to category {category} on {date.strftime('%Y-%m-%d %H:%M')}")


def os_command(text, send_message_cb=None, command=None, **_):
    assert command is not None
    cmd = f"{command} {text}"
    ec, out = subprocess.getstatusoutput(cmd)
    assert ec == 0, (cmd, ec, out)
    send_message_cb(f"```{out}```", parse_mode="Markdown")


_SLEEP_CATS = ["sleeping", "social"]


def sleepend(_, send_message_cb=None, mongo_client=None):
    mongo_coll = mongo_client[_common.MONGO_COLL_NAME]["alex.sleepingtimes"]
    last_record = mongo_coll.find_one(
        sort=[("startsleep", pymongo.DESCENDING)])
    cat = last_record["category"]
    if _common.get_sleeping_state(mongo_client) is None:
        send_message_cb("not sleeping")
        return
    _now = datetime.now()
    mongo_coll.update_one(
        {"startsleep": last_record["startsleep"]}, {"$set": {"endsleep": _common.to_utc_datetime(_now)}})
    heartbeat_time.SendKeyboard(
        mongo_url=os.environ["MONGO_URL"], is_create_bot=False).sanitize_mongo(cat)
    send_message_cb(
        f"end sleeping \"{cat}\" (was sleeping {(_now-timedelta(hours=9))-last_record['startsleep']})")


def ttask(content, send_message_cb=None, mongo_client=None):
    mongo_client[_common.MONGO_COLL_NAME]["alex.ttask"].insert_one({
        "content": content,
        "date": _common.to_utc_datetime(),
    })
    send_message_cb(f"log \"{content}\"")


def sleepstart(cat, send_message_cb=None, mongo_client=None):
    if cat not in _SLEEP_CATS:
        send_message_cb(
            f"cat \"{cat}\" not in \"{','.join(_SLEEP_CATS)}\"")
        return
    elif _common.get_sleeping_state(mongo_client) is not None:
        send_message_cb(f"already sleeping!")
        return
    elif mongo_client[_common.MONGO_COLL_NAME]["alex.time"].find_one(sort=[("date", pymongo.DESCENDING)]).get("category", None) is None:
        send_message_cb(f"waiting for time reply!")
        return

    mongo_coll = mongo_client[_common.MONGO_COLL_NAME]["alex.sleepingtimes"]
    mongo_coll.insert_one(
        {"category": cat, "startsleep": _common.to_utc_datetime()})
    send_message_cb(f"start sleeping \"{cat}\"")


def note(content, send_message_cb=None, mongo_client=None):
    mongo_client[_common.MONGO_COLL_NAME]["alex.notes"].insert_one({
        "content": content,
        "date": _common.to_utc_datetime(),
    })
    send_message_cb(f"note \"{content}\"")


def _rand(length, code):
    d = {}
    for _s in [string.ascii_lowercase, string.ascii_uppercase, string.digits]:
        for x in _s:
            d[x] = _s
    s = set()
    for x in code:
        s.add(d[x])
    s = "".join(s)
    return "".join(random.choices(s, k=length))


def rand(content, send_message_cb=None, mongo_client=None):
    length, code = re.split(r"\s+", content)
    send_message_cb(f"`{_rand(int(length),code)}`", parse_mode="Markdown")
