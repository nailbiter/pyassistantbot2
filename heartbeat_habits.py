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
import schedule
from datetime import datetime, timedelta
import logging
import time
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, MessageHandler, Filters
from pymongo import MongoClient
import _common
import pandas as pd
from croniter import croniter

_START_DATE = datetime(2021, 12, 14)


class SendKeyboard():
    def __init__(self, token, chat_id, mongo_url, create_bot=False):
        self._chat_id = chat_id
        self._bot = None
        if create_bot:
            updater = Updater(token, use_context=True)
            self._bot = updater.bot
        self._mongo_client = MongoClient(mongo_url)
        self._logger = logging.getLogger(self.__class__.__name__)

    def __call__(self):
        assert self._bot is not None
        _now = datetime.now()
        print(f"working at {_now.isoformat()}")

        habits_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.habits"]
        habits_df = pd.DataFrame(habits_coll.find({"enabled": True, }))
        print(habits_df)
        # sanity checks
        assert len(habits_df) == habits_df.name.nunique()

        habits_punch_df = []
        for habit in habits_df.to_dict(orient="records"):
            if "start_date" in habit:
                base = habit["start_date"]+timedelta(hours=9)
            else:
                base = _START_DATE
            it = croniter(habit["cronline"], base)
            while (ds := it.get_next(datetime)) <= _now:
                habits_punch_df.append({
                    **{k: habit[k] for k in ["name", "onFailed"]},
                    "date": ds-timedelta(hours=9),
                    "due": ds+timedelta(minutes=habit["delaymin"])-timedelta(hours=9),
                })
        habits_punch_df = pd.DataFrame(habits_punch_df)
        print(habits_punch_df)

        habits_punch_coll = self._get_habits_punch_coll()
        modified_count, matched_count = [0]*2
        upserts = []
        for dhp in habits_punch_df.to_dict(orient="records"):
            result = habits_punch_coll.update_one(
                {k: dhp[k] for k in ["name", "date"]},
                {"$set": {k: dhp[k] for k in ["due", "onFailed"]}},
                upsert=True,
            )
            modified_count += result.modified_count
            matched_count += result.matched_count
            if result.upserted_id is not None:
                upserts.append({**dhp, "mongo_id": result.upserted_id})

        upserts_df = pd.DataFrame(upserts)
        click.echo(f"upsert: {pd.DataFrame(upserts)}")
        click.echo(f"{matched_count} matched, {modified_count} modified")

        if len(upserts_df) > 0:
            upserts_df.due += timedelta(hours=9)
            upserts_df.due = upserts_df.due.apply(
                lambda ds: ds.strftime("%Y-%m-%d %H:%S"))
            self._send_message(
                f"don't forget to execute!:\n```{upserts_df[['name','due']]}```", parse_mode="Markdown")
#        for r in upserts_df.to_dict(orient="records"):
#            self._send_message(
#                text=f"don't forget to execute: \"{r['name']}\" before {(r['due']+timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')}!")

        self._sanitize_mongo()

    def _send_message(self, text, **kwargs):
        mess = self._bot.sendMessage(
            chat_id=self._chat_id,
            text=text,
            **kwargs
        )

    def _get_habits_punch_coll(self):
        habits_punch_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.habitspunch2"]
        return habits_punch_coll

    def get_pending_habits(self):
        coll = self._get_habits_punch_coll()
        _now = datetime.now()-timedelta(hours=9)
        pending_habits_df = pd.DataFrame(coll.find({"$and": [
            {"due": {"$gt": _now}},
            {"status": {"$exists": False}},
        ]}))
        pending_habits_df = pending_habits_df.drop(columns=["_id"])
        pending_habits_df = pending_habits_df.sort_values(by="due")
        pending_habits_df.date += timedelta(hours=9)
        pending_habits_df.due += timedelta(hours=9)
        return pending_habits_df

    def _sanitize_mongo(self):
        coll = self._get_habits_punch_coll()
        _now = datetime.now()
        habits_to_fail_df = pd.DataFrame(coll.find({"$and": [
            {"due": {"$lt": _common.to_utc_date(_now)}},
            {"status": {"$exists": False}},
        ]}))
        if len(habits_to_fail_df) > 0:
            self._send_message(
                f"you failed:\n```{habits_to_fail_df[['name']]}```", parse_mode="Markdown")
        for r in habits_to_fail_df.to_dict(orient="records"):
            coll.update_one(
                {k: r[k] for k in ["name", "date"]},
                {"$set": {"status": "FAILED"}},
            )

    def set_status(self, name, date, status):
        coll = self._get_habits_punch_coll()
        print(f"set status \"{status}\" for {(name,date)}")
        res = coll.update_one(
            {"name": name, "date": date-timedelta(hours=9)},
            {"$set": {"status": status}},
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
    job = SendKeyboard(*[
        ctx.obj[k] for k in "telegram_token,chat_id,mongo_url".split(",")
    ], create_bot=True)

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
@click.option("-n", "--name", multiple=True)
@click.option("-s", "--status", type=click.Choice(["DONE"]), default="DONE")
@click.pass_context
def show_habits(ctx, index, status, name):
    job = SendKeyboard(*[
        ctx.obj[k] for k in "telegram_token,chat_id,mongo_url".split(",")
    ])
    df = job.get_pending_habits()
    click.echo(df.to_string())
    click.echo(f"{len(df)} habits")

#    print(df.loc[0])
    new_idxs = [df[[_n.startswith(n)
                    for _n in df.name]].index[0] for n in name]
#    print(new_idxs)
#    exit(0)
    for i in set(index+new_idxs):
        r = df.loc[i]
        job.set_status(r["name"], r.date.to_pydatetime(), status)


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    heartbeat_habits()
