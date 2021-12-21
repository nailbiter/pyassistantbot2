#!/usr/bin/env python3
"""===============================================================================

        FILE: ./actor.py

       USAGE: ././actor.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-12-14T11:21:14.144121
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, MessageHandler, Filters
import pymongo
from datetime import datetime, timedelta
import _common
import subprocess
import _actor
import heartbeat_time


class Callback:
    def __init__(self, chat_id, mongo_url, bot):
        self._chat_id = chat_id
        self._mongo_client = pymongo.MongoClient(mongo_url)
        self._bot = bot

    def __call__(self, update, context):
        _now = datetime.now()
        chat_id = update.effective_message.chat_id
        if chat_id != self._chat_id:
            logging.warning(f"spurious message from {chat_id} ==> ignore")
            return

        message_id = update.callback_query.message.message_id
        data = int(update.callback_query.data)

        mongo_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.time"]
        msg = mongo_coll.find_one(
            {"telegram_message_id": message_id})
        if msg is None:
            logging.error(
                f"could not find keyboard for message_id={message_id} ==> ignore")
            return
        elif msg["category"] is not None:
            logging.warning(
                f"already have saved state \"{msg['category']}\" for message_id={message_id} ==> ignore")
            return
        time_category = _common.TIME_CATS[data]
        print(time_category)
        mongo_coll.update_one(
            {"telegram_message_id": message_id}, {"$set": {"category": time_category}})

        self._bot.delete_message(
            chat_id,
            message_id
        )
        self._bot.sendMessage(chat_id=chat_id, text=f"""
got: {time_category}
remaining time to live: {str(datetime(1991+70,12,24)-_now)} 
        """.strip())


class ProcessCommand:
    def __init__(self, chat_id, mongo_url, bot):
        self._chat_id = chat_id
        self._bot = bot
        self._mongo_client = pymongo.MongoClient(mongo_url)
        self._mongo_url = mongo_url

    def __call__(self, update, context):
        chat_id = update.effective_message.chat_id
        _now = datetime.now()

        if chat_id != self._chat_id:
            logging.warning(
                f"spurious message {update.message} from {chat_id} ==> ignore")
            return

        text = update.message.text.strip()
        if text.startswith("/habits"):
            cmd = f"python3 heartbeat_habits.py show-habits {text[len('/habits'):]}"
            ec, out = subprocess.getstatusoutput(cmd)
            assert ec == 0, (cmd, ec, out)
            self._send_message(f"```{out}```", parse_mode="Markdown")
        elif text.startswith("/done"):
            stripped = text[len("/done"):]
            # TODO
        elif text.startswith("/sleepstart"):
            cat = text[len("/sleepstart"):].strip()

            _SLEEP_CATS = ["sleeping", "social"]
            if cat not in _SLEEP_CATS:
                self._send_message(
                    f"cat \"{cat}\" not in \"{','.join(_SLEEP_CATS)}\"")
                return
            elif _common.get_sleeping_state(self._mongo_client) is not None:
                self._send_message(f"already sleeping!")
                return
            elif self._mongo_client[_common.MONGO_COLL_NAME]["alex.time"].find_one(sort=[("date", pymongo.DESCENDING)]).get("category", None) is None:
                self._send_message(f"waiting for time reply!")
                return

            mongo_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.sleepingtimes"]
            mongo_coll.insert_one(
                {"category": cat, "startsleep": _now-timedelta(hours=9)})
            self._send_message(f"start sleeping \"{cat}\"")
        elif text.startswith("/sleepend"):
            stripped = text[len("/sleepend"):].strip()
            mongo_coll = self._mongo_client[_common.MONGO_COLL_NAME]["alex.sleepingtimes"]
            last_record = mongo_coll.find_one(
                sort=[("startsleep", pymongo.DESCENDING)])
            cat = last_record["category"]
            if _common.get_sleeping_state(self._mongo_client) is None:
                self._send_message("not sleeping")
                return
            mongo_coll.update_one(
                {"startsleep": last_record["startsleep"]}, {"$set": {"endsleep": _now-timedelta(hours=9)}})
            heartbeat_time.SendKeyboard(
                mongo_url=self._mongo_url, is_create_bot=False).sanitize_mongo(cat)
            self._send_message(
                f"end sleeping \"{cat}\" (was sleeping {(_now-timedelta(hours=9))-last_record['startsleep']})")
        elif text.startswith("/money"):
            stripped = text[len("/money"):].strip()
            _actor.add_money(
                stripped, send_message_cb=self._send_message, mongo_client=self._mongo_client)
        else:
            logging.warning(
                f"unmatched message \"{text}\" ==> use default handler `add_money`")
            _actor.add_money(
                text, send_message_cb=self._send_message, mongo_client=self._mongo_client)

    def _send_message(self, text, **kwargs):
        mess = self._bot.sendMessage(
            chat_id=self._chat_id,
            text=text,
            **kwargs
        )

#        print(update.message.text,flush=True)


@click.command()
@click.option("-t", "--telegram-token", required=True, envvar="TELEGRAM_TOKEN")
@click.option("-c", "--chat-id", required=True, envvar="CHAT_ID", type=int)
@click.option("-m", "--mongo-url", required=True, envvar="MONGO_URL")
def actor(telegram_token, chat_id, mongo_url):
    logging.warning(datetime.now().isoformat())
    updater = Updater(telegram_token, use_context=True)
    bot = updater.bot
    pc = ProcessCommand(chat_id, mongo_url, bot)
    updater.dispatcher.add_handler(
        MessageHandler(filters=Filters.command, callback=pc))
    updater.dispatcher.add_handler(
        MessageHandler(
            filters=Filters.all,
            callback=lambda update, context: _actor.add_money(
                update.message.text.strip(), send_message_cb=pc._send_message, mongo_client=pc._mongo_client)
        )
    )
    edbp = Callback(chat_id, mongo_url, bot)
    updater.dispatcher.add_handler(
        CallbackQueryHandler(callback=edbp))
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    actor()
