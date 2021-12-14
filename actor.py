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
from pymongo import MongoClient
from datetime import datetime, timedelta
import common


class Callback:
    def __init__(self, telegram_token, chat_id, mongo_url, bot):
        self._chat_id = chat_id
        self._mongo_client = MongoClient(mongo_url)
        self._bot = bot

    def __call__(self, update, context):
        now_ = datetime.now()
        chat_id = update.effective_message.chat_id
        if chat_id != self._chat_id:
            logging.warning(f"spurious message from {chat_id} ==> ignore")
            return

        message_id = update.callback_query.message.message_id
        data = int(update.callback_query.data)

        mongo_coll = self._mongo_client[common.MONGO_COLL_NAME]["alex.time"]
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
        time_category = common.TIME_CATS[data]
        print(time_category)
        mongo_coll.update_one(
            {"telegram_message_id": message_id}, {"$set": {"category": time_category}})

        self._bot.delete_message(
            chat_id,
            message_id
        )
        self._bot.sendMessage(chat_id=chat_id, text=f"""
got: {time_category}
remaining time to live: {str(datetime(1991+70,12,24)-now_)} 
        """.strip())


@click.command()
@click.option("-t", "--telegram-token", required=True, envvar="TELEGRAM_TOKEN")
@click.option("-c", "--chat-id", required=True, envvar="CHAT_ID", type=int)
@click.option("-m", "--mongo-url", required=True, envvar="MONGO_URL")
def actor(telegram_token, chat_id, mongo_url):
    updater = Updater(telegram_token, use_context=True)
    bot = updater.bot
#    updater.dispatcher.add_handler(
#        MessageHandler(filters=Filters.all, callback=edbp))
    edbp = Callback(telegram_token, chat_id, mongo_url, bot)
    updater.dispatcher.add_handler(
        CallbackQueryHandler(callback=edbp))
    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    actor()
