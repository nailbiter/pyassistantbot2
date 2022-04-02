#!/usr/bin/env python3
"""===============================================================================

        FILE: heartbeat_time.py

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


class SendKeyboard():
    def __init__(self, mongo_url, token=None, chat_id=None, is_create_bot=True):
        if is_create_bot:
            updater = Updater(token, use_context=True)
            bot = updater.bot
            self._chat_id = chat_id
            self._bot = bot
        self._columns = 2
        self._keyboard = _common.TIME_CATS
        self._mongo_client = MongoClient(mongo_url)
        self._logger = logging.getLogger(self.__class__.__name__)

    def __call__(self):
        _now = datetime.now()
        print(f"working at {_now.isoformat()}", flush=True)
        sleeping_state = _common.get_sleeping_state(self._mongo_client)
        self._logger.warning(f"sleeping_state: {sleeping_state}")

        message_id = "FAILURE"
        try:
            if sleeping_state is None:
                mess = self._send_message("北鼻，你在幹什麼？",
                                          parse_mode="Markdown",
                                          reply_markup=InlineKeyboardMarkup([
                                              [
                                                     InlineKeyboardButton(
                                                         self._keyboard[i+j], callback_data=str(i+j))
                                                     for j in
                                                     range(self._columns)
                                                     if i+j < len(self._keyboard)
                                              ]
                                              for i
                                              in range(0, len(self._keyboard), self._columns)
                                          ]),
                                          )
            else:
                is_no_bother, cat = sleeping_state
                if not is_no_bother:
                    mess = self._send_message(f"""
    got: {cat}
    remaining time to live: {str(datetime(1991+70,12,24)-_now)} 
                """.strip())
            message_id = mess.message_id

        self._logger.warning("before sanitize")
        self.sanitize_mongo(
            "useless" if sleeping_state is None else sleeping_state[1])
        self._logger.warning("before insert")
        res = self._mongo_client[_common.MONGO_COLL_NAME]["alex.time"].insert_one({
            "date": _now,
            "category": None,
            "telegram_message_id": message_id if sleeping_state is None else sleeping_state[1],
        })
        self._logger.warning(f"after insert: {res.inserted_id}")

    def _send_message(self, text, **kwargs):
        mess = self._bot.sendMessage(
            chat_id=self._chat_id,
            text=text,
            **kwargs
        )
        return mess

    def sanitize_mongo(self, imputation_state):
        self._logger.warning(f"imputation_state: {imputation_state}")
        mongo_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.time"]
        empties = pd.DataFrame(mongo_coll.find({"category": None}))
#        print(empties)
        if len(empties) > 0:
            self._logger.warning(empties)
            # FIXME: optimize via `update_many`
            for _id in empties["_id"]:
                mongo_coll.update_one(
                    {"_id": _id},
                    {
                        "$set": {
                            "category": imputation_state,
                            "_last_modification_date": _common.to_utc_datetime(),
                        },
                    },
                )


_SCHEDULING_INTERVAL_MIN = 30


@click.command()
@click.option("-t", "--telegram-token", required=True, envvar="TELEGRAM_TOKEN")
@click.option("-c", "--chat-id", required=True, envvar="CHAT_ID", type=int)
@click.option("-m", "--mongo-url", required=True, envvar="MONGO_URL")
def heartbeat_time(telegram_token, chat_id, mongo_url):
    assert 60 % _SCHEDULING_INTERVAL_MIN == 0
    job = SendKeyboard(
        mongo_url, token=telegram_token, chat_id=chat_id)

    if not True:
        schedule.every(1).minutes.do(job)
    else:
        for i in range(0, 60, _SCHEDULING_INTERVAL_MIN):
            schedule.every().hour.at(f":{i:02d}").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    heartbeat_time()
