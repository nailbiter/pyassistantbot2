#!/usr/bin/env python3
"""===============================================================================

        FILE: heartbeat_habits.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION:

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION:
     VERSION: ---
     CREATED: 2021-12-14T10:42:48.216487
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import typing
import schedule
from datetime import datetime, timedelta
import logging
import time
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    Filters,
)
from pymongo import MongoClient
import _common
import pandas as pd
from croniter import croniter
import functools

_START_DATE = datetime(2021, 12, 14)


class SendKeyboard:
    def __init__(self, token, chat_id, mongo_url, create_bot=False):
        self._chat_id = chat_id
        self._bot = None
        if create_bot:
            updater = Updater(token, use_context=True)
            self._bot = updater.bot
        self._mongo_client = MongoClient(mongo_url)
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_anchor_dates(self):
        anchor_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.habits_anchors"]
        return {
            r["name"]: _common.to_utc_datetime(r["date"], inverse=True)
            for r in anchor_coll.find()
        }

    def __call__(self):
        assert self._bot is not None
        _now = datetime.now()
        print(f"working at {_now.isoformat()}")

        with _common.TimerContextManager("get habits"):
            habits_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.habits"]
            habits_df = pd.DataFrame(
                habits_coll.find(
                    {
                        "enabled": True,
                    }
                )
            )
            print(habits_df)
            # sanity checks
            assert len(habits_df) == habits_df.name.nunique()

        anchor_dates = self._get_anchor_dates()
        print(f"anchor_dates: {anchor_dates}")

        with _common.TimerContextManager("generate habits_punch_df"):
            habits_punch_df = []
            for habit in habits_df.to_dict(orient="records"):
                if habit["name"] in anchor_dates:
                    base = anchor_dates[habit["name"]] + timedelta(seconds=1)
                elif "start_date" in habit and not pd.isna(habit["start_date"]):
                    base = _common.to_utc_datetime(habit["start_date"], inverse=True)
                else:
                    base = _START_DATE

                print(f"base for {habit['name']} is {base.isoformat()}")
                it = croniter(habit["cronline"], base)
                while (ds := it.get_next(datetime)) <= _now:
                    habits_punch_df.append(
                        {
                            **{k: habit[k] for k in ["name", "onFailed", "info"]},
                            "date": ds - timedelta(hours=9),
                            "due": ds
                            + timedelta(minutes=habit["delaymin"])
                            - timedelta(hours=9),
                        }
                    )
            habits_punch_df = pd.DataFrame(habits_punch_df)
            print(habits_punch_df)

        with _common.TimerContextManager("generate and insert upserts"):
            # FIXME: this takes very long time
            habits_punch_coll = self._get_habits_punch_coll()
            modified_count, matched_count = [0] * 2
            upserts = []
            for dhp in habits_punch_df.to_dict(orient="records"):
                result = habits_punch_coll.update_one(
                    {k: dhp[k] for k in ["name", "date"]},
                    {"$set": {k: dhp[k] for k in ["due", "onFailed", "info"]}},
                    upsert=True,
                )
                modified_count += result.modified_count
                matched_count += result.matched_count
                if result.upserted_id is not None:
                    upserts.append({**dhp, "mongo_id": result.upserted_id})

        upserts_df = pd.DataFrame(upserts)
        click.echo(f"upsert: {pd.DataFrame(upserts)}")
        click.echo(f"{matched_count} matched, {modified_count} modified")

        anchor_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.habits_anchors"]
        updated_habits = set() if len(upserts_df) == 0 else set(upserts_df.name)
        for habit in habits_df.to_dict(orient="records"):
            if habit["name"] in updated_habits:
                print(f"update anchor for {habit['name']}")
                anchor_coll.update_one(
                    {"name": habit["name"]},
                    {"$set": {"date": _common.to_utc_datetime(_now)}},
                    upsert=True,
                )

        with _common.TimerContextManager("sending messages"):
            if len(upserts_df) > 0:
                #                upserts_df.due += timedelta(hours=9)
                upserts_df.due = upserts_df.due.apply(
                    functools.partial(_common.to_utc_datetime, inverse=True)
                )
                upserts_df.due = upserts_df.due.apply(
                    lambda ds: ds.strftime("%Y-%m-%d %H:%M")
                )
                upserts_df = upserts_df[["name", "due", "info"]]
                #            for k in ["date", "duo"]:
                #                upserts_df[k] = upserts_df[k].apply(
                #                    lambda ds: ds.strftime("%Y-%m-%d %H:%M"))
                if len(upserts_df) > 1:
                    self._send_message(
                        f"don't forget to execute!:\n```{upserts_df[['name','due']]}```",
                        parse_mode="Markdown",
                    )
                else:
                    self._send_message(
                        f"don't forget to execute!: ```{upserts_df.to_dict(orient='records')[0]}```",
                        parse_mode="Markdown",
                    )

        with _common.TimerContextManager("sanitize mongo"):
            self._sanitize_mongo()

    def _send_message(self, text, **kwargs):
        mess = self._bot.sendMessage(chat_id=self._chat_id, text=text, **kwargs)

    def _get_habits_punch_coll(self):
        habits_punch_coll = self._mongo_client[_common.MONGO_COLL_NAME][
            "alex.habitspunch2"
        ]
        if True:
            _df = pd.DataFrame(habits_punch_coll.find())
            #            logging.warning("sanity check")
            _df = pd.DataFrame(
                [
                    {"name": n, "date": d, "cnt": len(slice_)}
                    for (n, d), slice_ in _df.groupby(["name", "date"])
                ]
            )
            assert len(_df.query("cnt>1")) == 0, _df.query("cnt>1").head()
        return habits_punch_coll

    def get_habits(
        self,
        which: typing.Literal["PENDING", "FAILED"] = "PENDING",
        is_use_remote_filter: bool = False,
        remote_filter_prefixes: list[str] = [],
    ):
        coll = self._get_habits_punch_coll()
        _now = datetime.now() - timedelta(hours=9)
        if which == "PENDING":
            filter_ = {
                "$and": [
                    {"due": {"$gt": _now}},
                    {"status": {"$exists": False}},
                ]
            }
        elif which == "FAILED":
            filter_ = {
                "$and": [
                    {"due": {"$lt": _now}},
                    {"status": {"$ne": "DONE"}},
                    {"status": {"$exists": True}},
                    {"onFailed": {"$ne": "remove"}},
                    *(
                        [
                            {
                                "$or": [
                                    {"name": {"$regex": f"^{prefix}.*"}}
                                    for prefix in remote_filter_prefixes
                                ]
                            }
                        ]
                        if is_use_remote_filter
                        else []
                    ),
                ]
            }
        else:
            raise NotImplementedError(which)
        logging.warning(filter_)
        pending_habits_df = pd.DataFrame(coll.find(filter_))
        #        pending_habits_df = pending_habits_df.drop(columns=["_id"])
        pending_habits_df = pending_habits_df.sort_values(by="due")
        pending_habits_df.date += timedelta(hours=9)
        pending_habits_df.due += timedelta(hours=9)
        return pending_habits_df

    def _sanitize_mongo(self):
        coll = self._get_habits_punch_coll()
        _now = datetime.now()
        habits_to_fail_df = pd.DataFrame(
            coll.find(
                {
                    "$and": [
                        {"due": {"$lt": _common.to_utc_datetime(_now)}},
                        {"status": {"$exists": False}},
                    ]
                }
            )
        )
        if len(habits_to_fail_df) > 0:
            self._send_message(
                f"you failed:\n```{habits_to_fail_df[['name']]}```",
                parse_mode="Markdown",
            )
        for r in habits_to_fail_df.to_dict(orient="records"):
            coll.update_one(
                {k: r[k] for k in ["name", "date"]},
                {"$set": {"status": "FAILED"}},
            )

    def set_status(self, _id, status, name=None, date=None):
        coll = self._get_habits_punch_coll()
        print(f'set status "{status}" for {(_id,name,date)}')
        res = coll.update_one(
            {"_id": _id},
            {
                "$set": {
                    "status": status,
                    "_last_modification": _common.to_utc_datetime(),
                }
            },
        )
        print((res.matched_count, res.modified_count))


@click.group()
@click.option("-t", "--telegram-token", required=True, envvar="TELEGRAM_TOKEN")
@click.option("-c", "--chat-id", required=True, envvar="CHAT_ID", type=int)
@click.option("-m", "--mongo-url", required=True, envvar="MONGO_URL")
@click.pass_context
def heartbeat_habits(ctx, **kwargs):
    logging.warning(datetime.now().isoformat())
    ctx.ensure_object(dict)
    for k, v in kwargs.items():
        ctx.obj[k] = v


@heartbeat_habits.command()
@click.pass_context
def heartbeat(ctx):
    job = SendKeyboard(
        *[ctx.obj[k] for k in "telegram_token,chat_id,mongo_url".split(",")],
        create_bot=True,
    )

    if not False:
        schedule.every(1).minutes.do(job)
    else:
        for i in range(0, 60, 2):
            schedule.every().hour.at(f":{i:02d}").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


@heartbeat_habits.command()
@click.option("-i", "--index", type=int, multiple=True)
@click.option("-n", "--name", "names", multiple=True)
@click.option("-s", "--status", type=click.Choice(["DONE"]), default="DONE")
@click.option("--show-failed/--no-show-failed", "-f/ ", default=False)
@click.option("-c", "--count", default=1, type=int)
@click.option("--dry-run/--no-dry-run", default=False)
@click.option("--use-remote-prefix/--no-use-remote-prefix", " /-l", default=True)
@click.pass_context
def show_habits(
    ctx, index, status, names, show_failed, count, dry_run, use_remote_prefix
):
    job = SendKeyboard(
        *[ctx.obj[k] for k in "telegram_token,chat_id,mongo_url".split(",")]
    )

    if not show_failed:
        df = job.get_habits(
            which="PENDING",
        )
        df["_ids"] = df.pop("_id").apply(lambda x: [x])
        l = len(df)
    else:
        df = job.get_habits(
            which="FAILED",
            is_use_remote_filter=use_remote_prefix and (len(names) > 0),
            remote_filter_prefixes=list(names),
        )

        df = pd.DataFrame(
            [
                {
                    "name": n,
                    "cnt": len(slice_),
                    "date": slice_.date.min(),
                    "_ids": list(slice_["_id"]),
                }
                for n, slice_ in df.groupby("name")
            ]
        )
        l = df["cnt"].sum()
    _df = df.copy()
    for k in ["date", "due"]:
        if k in list(_df):
            _df[k] = _df[k].apply(lambda ds: ds.strftime("%Y-%m-%d %H:%M"))
    click.echo(_df.drop(columns=["_ids"]).to_string())
    click.echo(f"{l} habits")

    #    print(df.loc[0])
    #    new_idxs = [df[[_n.startswith(n)
    #                    for _n in df.name]].index[0] for n in name]
    new_idxs = []
    for n in names:
        idxs = df[[_n.startswith(n) for _n in df.name]].index
        assert len(idxs) == 1, (n, idxs)
        new_idxs.append(idxs[0])

    remaining_counts = {}
    for i in set(list(index) + new_idxs):
        r = df.loc[i]
        _count = len(r["_ids"]) if count == 0 else count
        remaining_counts[r["name"]] = len(r["_ids"]) - _count
        for _id in r["_ids"][:_count]:
            _kwargs = {
                "_id": _id,
                "status": status,
                "name": r["name"],
                "date": r.date.to_pydatetime(),
            }
            if dry_run:
                logging.warning(f"dry_run: {_kwargs}")
            else:
                job.set_status(**_kwargs)
    if show_failed and len(list(index) + new_idxs) > 0:
        print(f"remains: {remaining_counts}")


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    heartbeat_habits()
