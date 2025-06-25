#!/usr/bin/env python3
"""===============================================================================

        FILE: money2.py

       USAGE: ./money2.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-05-23T10:59:57.025070
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
import pymongo
from datetime import datetime, timedelta
from pytz import timezone
import pandas as pd
from bson.codec_options import CodecOptions


def _get_coll(mongo_url):
    client = pymongo.MongoClient(mongo_url)
    coll = client.logistics["alex.money"].with_options(
        codec_options=CodecOptions(tz_aware=True, tzinfo=timezone("Asia/Tokyo"))
    )
    return coll


@click.command()
@click.option("--mongo-url", envvar="MONGO_URL", required=True)
@click.option("-t", "--tags", multiple=True)
def money2(mongo_url, tags):
    coll = _get_coll(mongo_url)
    money_df = pd.DataFrame(coll.find())
    if len(tags) > 0:
        money_df = money_df[~money_df.tags.isna()]
        money_df = money_df[[(set(tags) <= set(_tags)) for _tags in money_df.tags]]

    money_df = money_df.sort_values(by="date", ascending=False)
    click.echo(money_df)


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    money2()
