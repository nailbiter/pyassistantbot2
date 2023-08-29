"""===============================================================================

        FILE: _ttask.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-06-06T17:53:57.791298
    REVISION: ---

==============================================================================="""
from dotenv import load_dotenv
import os
from os import path
import logging
import _common
import pymongo
import pandas as pd
from datetime import datetime, timedelta
import operator
import inspect
import types
from typing import cast
import subprocess
import functools
import click
import typing


def ttask(
    mongo_url,
    head=None,
    grep=None,
    is_print=True,
    out_format: typing.Literal["json", "plain"] = "plain",
):
    client = pymongo.MongoClient(mongo_url)
    coll = client[_common.MONGO_COLL_NAME]["alex.ttask"]
    df = pd.DataFrame(
        coll.find(
            filter={"status": {"$ne": "DONE"}}, sort=[("date", pymongo.DESCENDING)]
        )
    )
    if len(df) == 0:
        click.echo("all done!")
        exit(0)
    df.date = df.date.apply(functools.partial(_common.to_utc_datetime, inverse=True))
    if grep is not None:
        df = df[df.content.apply(lambda s: grep in s)]
    l = len(df)
    if head is not None:
        df = df.head(head)
    if is_print:
        if out_format == "plain":
            click.echo(df.drop(columns=["_id"]).to_string())
            click.echo(f"{l} tasks")
        elif out_format == "json":
            click.echo(
                pd.DataFrame(
                    {
                        **df,
                        "_id": df["_id"].apply(str),
                        "date": df["date"].apply(operator.methodcaller("isoformat")),
                    }
                ).reset_index().to_json(orient="records")
            )
        else:
            raise NotImplementedError(dict(out_format=out_format))
    return df, coll
