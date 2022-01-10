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
import inspect
import types
from typing import cast


@click.command()
@click.option("-i", "--index", type=int, multiple=True)
@click.option("--mongo-url", envvar="MONGO_URL", required=True)
@click.option("-g", "--gstasks-line")
def ttask(index, mongo_url, gstasks_line):
    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(
        types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

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
        if gstasks_line is not None:
            cmd = f"./gstasks.py add -n \"{r.content}\" {gstasks_line}"
            logger.warning(f"> {cmd}")
            os.system(cmd)


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    ttask()
