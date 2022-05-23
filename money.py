#!/usr/bin/env python3
"""===============================================================================

        FILE: money.py

       USAGE: ./money.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-02-15T17:30:47.814276
    REVISION: ---

==============================================================================="""

import click
from datetime import datetime, timedelta
from _common import get_remote_mongo_client
from pytz import timezone
from bson.codec_options import CodecOptions
import pandas as pd
import numpy as np
import pymongo
import logging
import json
import requests
from os import path
from dotenv import load_dotenv


def _get_coll(mongo_pass):
    client = get_remote_mongo_client(mongo_pass)
    coll = client.logistics["alex.money"].with_options(
        codec_options=CodecOptions(tz_aware=True, tzinfo=timezone('Asia/Tokyo')))
    return coll


def _add_logger(f):
    logger = logging.getLogger(f.__name__)

    def _f(*args, **kwargs):
        return f(*args, logger=logger, **kwargs)
    _f.__name__ = f.__name__
    return _f


def _get_last_week_boundaries(day):
    """
    return (last_week_start_inc,last_week_end_exc) <-- both mondays
    """
    last_week_start_inc = day-timedelta(days=7)
    last_week_start_inc = last_week_start_inc.replace(
        hour=0, minute=0, second=0, microsecond=0)
    last_week_start_inc -= timedelta(days=last_week_start_inc.weekday())
    return last_week_start_inc, last_week_start_inc+timedelta(days=7)


@click.group()
@click.option("--debug/--no-debug", default=False)
def money(debug):
    if debug:
        logging.basicConfig(level=logging.INFO)


@money.command()
@click.option("--mongo-pass", envvar="MONGO_PASS", required=True)
def tags(mongo_pass):
    coll = _get_coll(mongo_pass)
    df = pd.DataFrame(coll.find())
    tags = {}
    df = df[[isinstance(t, list) for t in df.tags]]
    for r in df.to_dict(orient="records"):
        for t in r["tags"]:
            tags[t] = tags.get(t, 0) + 1
    df = pd.DataFrame([{"tag": k, "count": v}for k, v in tags.items()])
    click.echo(df.sort_values(by="tag").to_string())


@money.command()
@click.option("-d", "--day", type=click.DateTime(["%Y-%m-%d"]))
@click.option("-m", "--mode", type=click.Choice(["daily", "monthly", "weekly"]), default="daily")
@click.option("--mongo-pass", envvar="MONGO_PASS", required=True)
@click.option("--monthly-regular-payments-file-name", type=click.Path(), default=".monthly_regular_payments.json")
@click.option("--monthly-channel-webhook", envvar="MONTHLY_CHANNEL_WEBHOOK")
@click.option("--weekly-channel-webhook", envvar="WEEKLY_CHANNEL_WEBHOOK")
@click.option("--send-slack-message/--no-send-slack-message", default=True)
@click.option("--show-top-expenses", type=int)
@_add_logger
def show(day, mongo_pass, mode, monthly_regular_payments_file_name, monthly_channel_webhook, send_slack_message, weekly_channel_webhook, show_top_expenses, logger=None):
    if day is None:
        day = datetime.now()
    coll = _get_coll(mongo_pass)
    if mode == "daily":
        money_df = pd.DataFrame(
            coll.find({"$and": [{"date": {"$gte": datetime(day.year, day.month, day.day)}}, {"date": {"$lt": datetime(day.year, day.month, day.day)+timedelta(days=1)}}]}, sort=[("date", pymongo.DESCENDING)]))
        click.echo(money_df.to_string())
        click.echo(money_df.groupby("category").sum())
    elif mode == "monthly":
        assert monthly_channel_webhook is not None
        next_month = [day.year, day.month]
        next_month[1] += 1
        if next_month[1] >= 12:
            next_month[1] = next_month[1] % 12
            next_month[0] += 1
        money_df = pd.DataFrame(
            coll.find({"$and": [{"date": {"$gte": datetime(day.year, day.month, 1)}}, {"date": {"$lt": datetime(*next_month, 1)+timedelta(days=1)}}]}, sort=[("date", pymongo.DESCENDING)]))
        money_df = money_df[[
            d.month == day.month and d.year == d.year for d in money_df["date"]]]
        with open(monthly_regular_payments_file_name) as f:
            monthly_regular_payments = json.load(f)
        for r in monthly_regular_payments:
            _r = {
                **{k: v for k, v in r.items() if k not in ["date", "tags"]},
                "date": datetime(day.year, day.month, **r["date"]),
                "tags": ["regular"]+r.get("tags", []),
            }
            money_df = money_df.append({
                **_r,
                **{"_id": "***regular***"},
            }, ignore_index=True)
            # FIXME: make sure it is idempotent
            coll.insert_one(_r)
        logger.info(money_df.to_csv())

        money_df = money_df.groupby("category").agg({"amount": np.sum})
        money_df = money_df.reset_index()
        money_df = money_df.append(
            {"category": "_total", "amount": money_df.amount.sum()}, ignore_index=True)
        money_df = money_df.set_index("category")
        click.echo(money_df.to_string())
        if send_slack_message:
            requests.post(monthly_channel_webhook, json.dumps({
                "text": f"""```{day.strftime("%Y-%m")}\n{money_df.to_string()}```"""
            }),
                headers={
                    "Content-type": "application/json"
            })
    elif mode == "weekly":
        assert weekly_channel_webhook is not None
        last_week_start_inc, last_week_end_exc = _get_last_week_boundaries(day)
        money_df = pd.DataFrame(
            coll.find({"$and": [{"date": {"$gte": last_week_start_inc}}, {"date": {"$lt": last_week_end_exc}}]}, sort=[("date", pymongo.DESCENDING)]))
        logger.info(money_df.to_csv())

        if show_top_expenses is not None:
            show_top_expenses = money_df.sort_values(by="amount", ascending=False)[
                :show_top_expenses]
        money_df = money_df.groupby("category").agg({"amount": np.sum})
        money_df = money_df.reset_index()
        money_df = money_df.append(
            {"category": "_total", "amount": money_df.amount.sum()}, ignore_index=True)
        money_df = money_df.set_index("category")
        click.echo(money_df.to_string())
        if show_top_expenses is not None:
            click.echo(show_top_expenses)
        if send_slack_message:
            text = f"""{day.strftime("%Y-%m")}\n{money_df.to_string()}"""
            if show_top_expenses is not None:
                text += "\n"+show_top_expenses.to_string()
            text = f"```{text}```"
            requests.post(weekly_channel_webhook, json.dumps({
                "text": text,
            }),
                headers={
                    "Content-type": "application/json"
            })
    else:
        raise NotImplementedError(f"unknown mode \"{mode}\"")


if __name__ == "__main__":
    if path.isfile(".envrc"):
        logging.warning("loading .envrc")
        load_dotenv(".envrc")
    money()
