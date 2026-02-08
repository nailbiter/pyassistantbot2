import click

# from dotenv import load_dotenv
import os
from os import path
import logging
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
import pymongo
from datetime import datetime, timedelta
import _common
import subprocess
import _actor
import re
import functools
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi import FastAPI, Request, Response
import typing

from alex_leontiev_toolbox_python.utils.logging_helpers import get_configured_logger

logger = get_configured_logger(
    "actor",
    log_format="%(asctime)s - %(name)s - %(levelname)s - Line:%(lineno)d - %(message)s",
)

# --- ORIGINAL CLASSES (RETAINED) ---


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
        msg = mongo_coll.find_one({"telegram_message_id": message_id})
        if msg is None:
            logging.error(
                f"could not find keyboard for message_id={message_id} ==> ignore"
            )
            return
        elif msg["category"] is not None:
            logging.warning(
                f"already have saved state \"{msg['category']}\" for message_id={message_id} ==> ignore"
            )
            return
        time_category = _common.TIME_CATS[data]
        print(time_category)
        # FIXME: use `sanitize_mongo` of `heartbeat_time`
        mongo_coll.update_one(
            {"telegram_message_id": message_id},
            {
                "$set": {
                    "category": time_category,
                    "_last_modification_date": _common.to_utc_datetime(),
                }
            },
        )

        self._bot.delete_message(chat_id, message_id)
        self._bot.sendMessage(
            chat_id=chat_id,
            text=f"""
got: {time_category}
remaining time to live: {str(datetime(1991+70,12,24)-_now)} 
        """.strip(),
        )


class ProcessCommand:
    def __init__(self, chat_id, mongo_url, bot, commands={}):
        self._chat_id = chat_id
        self._bot = bot
        self._mongo_client = pymongo.MongoClient(mongo_url)
        self._mongo_url = mongo_url
        self._commands = commands

    def __call__(self, update=None, context=None, text=None, chat_id=None):
        chat_id = update.effective_message.chat_id if chat_id is None else chat_id
        _now = datetime.now()

        text = text if text is not None else update.message
        if chat_id != self._chat_id:
            logging.warning(f"spurious message {text} from {chat_id} ==> ignore")
            return

        try:
            text = text.strip()
            for command, callback in self._commands.items():
                if text.startswith(f"/{command}"):
                    stripped = text[len(command) + 1 :].strip()
                    callback(
                        stripped,
                        send_message_cb=self._send_message,
                        mongo_client=self._mongo_client,
                    )
                    return
            if text.startswith("/"):
                cmd, *_ = re.split(r"\s+", text)
                raise Exception(f"unknown command {cmd}")

            logging.warning(f'unmatched message "{text}" ==> use default handler')
            self._commands[None](
                text,
                send_message_cb=self._send_message,
                mongo_client=self._mongo_client,
            )
        except Exception as e:
            self._send_message(f"exception: ``` {e}```", parse_mode="Markdown")
            raise e

    def _send_message(self, text, **kwargs):
        mess = self._bot.sendMessage(chat_id=self._chat_id, text=text, **kwargs)


# --- CLOUD RUN / FASTAPI REFACTORING ---


def run_telegram_bot(text: str, chat_id: typing.Optional[int] = None) -> None:
    """Logic moved from original actor() to run in a background thread"""
    # if path.isfile(".env"):
    #     logging.warning("loading .env")
    #     load_dotenv()

    telegram_token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = int(os.environ.get("CHAT_ID")) if chat_id is None else int(chat_id)
    mongo_url = os.environ.get("MONGO_URL")
    logger.debug(
        dict(telegram_token=telegram_token, chat_id=chat_id, mongo_url=mongo_url)
    )

    logging.warning(f"Bot thread starting at {datetime.now().isoformat()}")

    updater = Updater(telegram_token, use_context=True)
    bot = updater.bot
    pc = ProcessCommand(
        chat_id,
        mongo_url,
        bot,
        commands={
            # "money": _actor.add_money,
            "habits": functools.partial(
                _actor.os_command, command="python3 heartbeat_habits.py show-habits"
            ),
            # "tasks": functools.partial(_actor.os_command, command="python3 task.py s"),
            # "tasknew": functools.partial(
            #     _actor.os_command, command="python3 task.py n"
            # ),
            # "taskmodify": functools.partial(
            #     _actor.os_command, command="python3 task.py m"
            # ),
            # **{
            #     k: getattr(_actor, k)
            #     for k in "sleepstart,sleepend,ttask,note,rand,nutrition".split(",")
            # },
            # None: _actor.ttask,
        },
    )
    # updater.dispatcher.add_handler(MessageHandler(filters=Filters.all, callback=pc))
    # edbp = Callback(chat_id, mongo_url, bot)
    # updater.dispatcher.add_handler(CallbackQueryHandler(callback=edbp))

    logger.debug(dict(text=text, chat_id=chat_id))
    pc(text=text, chat_id=chat_id)

    # logging.warning("Telegram polling started.")
    # updater.start_polling()
    # updater.idle()


# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     """Handles startup/shutdown logic for the FastAPI server"""
#     # Start the telegram bot in a background thread so the web server can bind to $PORT
#     bot_thread = threading.Thread(target=run_telegram_bot, daemon=True)
#     bot_thread.start()
#     yield
#     logging.warning("Shutting down FastAPI app...")


# This 'app' object is what Gunicorn/Uvicorn will look for
app = FastAPI(
    # lifespan=lifespan
)


@app.post("/")
async def health_check(request: Request):
    """Standard health check for Cloud Run"""
    logger.debug(request)

    data = await request.json()
    logger.debug(dict(data=data, t=type(data)))

    from_chat_id = int(data["message"]["from"]["id"])
    assert from_chat_id == int(os.environ["CHAT_ID"]), (
        from_chat_id,
        int(os.environ["CHAT_ID"]),
    )
    message = data["message"]["text"]

    run_telegram_bot(message, from_chat_id)

    return {"status": "ok", "bot_running": True}


# # Maintain original CLI compatibility just in case you run it locally
# @click.command()
# @click.option("-t", "--telegram-token", required=True, envvar="TELEGRAM_TOKEN")
# @click.option("-c", "--chat-id", required=True, envvar="CHAT_ID", type=int)
# @click.option("-m", "--mongo-url", required=True, envvar="MONGO_URL")
# def actor(telegram_token, chat_id, mongo_url):
#     run_telegram_bot()


# if __name__ == "__main__":
#     # If running directly (not via Gunicorn), use the original CLI
#     actor()
