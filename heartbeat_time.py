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
    def __init__(self, token, chat_id, mongo_url):
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
        elif sleeping_state == "NO_BOTHER":
            pass
        else:
            mess = self._send_message(f"""
got: {sleeping_state}
remaining time to live: {str(datetime(1991+70,12,24)-_now)} 
        """.strip())
#        print(mess.message_id)
        self._sanitize_mongo()
        self._mongo_client[_common.MONGO_COLL_NAME]["alex.time"].insert_one({
            "date": _now,
            "category": None,
            "telegram_message_id": mess.message_id,
        })

    def _send_message(self, text, **kwargs):
        mess = self._bot.sendMessage(
            chat_id=self._chat_id,
            text=text,
            **kwargs
        )
        return mess

    def _sanitize_mongo(self):
        mongo_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.time"]
        empties = pd.DataFrame(mongo_coll.find({"category": None}))
#        print(empties)
        if len(empties) > 0:
            self._logger.warning(empties)
            # FIXME: optimize via `update_many`
            for message_id in empties.telegram_message_id:
                mongo_coll.update_one({"telegram_message_id": message_id}, {
                                      "$set": {"category": "useless"}})


@click.command()
@click.option("-t", "--telegram-token", required=True, envvar="TELEGRAM_TOKEN")
@click.option("-c", "--chat-id", required=True, envvar="CHAT_ID", type=int)
@click.option("-m", "--mongo-url", required=True, envvar="MONGO_URL")
def heartbeat_time(telegram_token, chat_id, mongo_url):
    job = SendKeyboard(telegram_token, chat_id, mongo_url)

    if not True:
        schedule.every(1).minutes.do(job)
    else:
        schedule.every().hour.at(":30").do(job)
        schedule.every().hour.at(":00").do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    heartbeat_time()
