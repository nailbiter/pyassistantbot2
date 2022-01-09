#!/usr/bin/env python3
"""===============================================================================

        FILE: ttask.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-12-24T20:14:55.180457
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
import _common
import pymongo
import pandas as pd
from datetime import datetime, timedelta


@click.command()
@click.option("-i", "--index", type=int, multiple=True)
@click.option("--mongo-url", envvar="MONGO_URL", required=True)
def ttask(index, mongo_url):
    client = pymongo.MongoClient(mongo_url)
    coll = client[_common.MONGO_COLL_NAME]["alex.ttask"]
    df = pd.DataFrame(coll.find(filter={"status": {"$ne": "DONE"}}, sort=[
                      ("date", pymongo.DESCENDING)]))
    if len(df) == 0:
        click.echo("all done!")
        return
    df.date += timedelta(hours=9)
    click.echo(df.drop(columns=["_id"]).to_string())
    for i in index:
        r = df.loc[i]
        coll.update_one({"_id": r._id}, {
                        "$set": {"status": "DONE", "_last_modification": _common.to_utc_datetime()}})
        click.echo(f"done {r._id} ({r.content})")


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    ttask()
