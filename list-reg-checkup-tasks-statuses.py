#!/usr/bin/env python3
"""===============================================================================

        FILE: list-reg-checkup-tasks-statuses.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-30T15:39:39.397414
    REVISION: ---

1. re-integrate in `gstasks`
2. check for tasks that are post-scheduled

==============================================================================="""

import json
import logging

# from dotenv import load_dotenv
import os
import re
from datetime import datetime, timedelta
from os import path

import click
import numpy as np
import pandas as pd
import pymongo

import _gstasks


@click.command()
@click.option("--mongo-url", envvar="PYASSISTANTBOT_MONGO_URL", required=True)
@click.option("-h", "--head", type=int)
@click.option("--debug/--no-debug", default=False)
@click.option("-n", "--name-max-length", type=int, default=20)
@click.option(
    "-o", "--out-format", type=click.Choice(["plain", "tsv", "json"]), default="plain"
)
@click.option(
    "-d",
    "--scheduled-date",
    "scheduled_dates",
    type=click.DateTime(["%Y-%m-%d"]),
    multiple=True,
)
def list_reg_checkup_tasks_statuses(
    mongo_url, head, debug, name_max_length, out_format, scheduled_dates
):
    if debug:
        logging.basicConfig(level=logging.INFO)
    client = pymongo.MongoClient(mongo_url)
    result = client.gstasks.regular_checkup.aggregate(
        [
            {
                "$lookup": {
                    "from": "tasks",
                    "localField": "uuid",
                    "foreignField": "uuid",
                    "as": "joinedResult",
                }
            }
        ]
    )
    df = pd.DataFrame(result)
    assert set(df.joinedResult.apply(len).unique()) == {
        1,
    }
    df = pd.DataFrame(
        [
            {
                **{k: v for k, v in r.items() if k != "joinedResult"},
                **r["joinedResult"][0],
            }
            for r in df.to_dict(orient="records")
        ]
    )

    # filtering and reduction
    df = df.query("status!='DONE' and status!='FAILED'")
    df = pd.DataFrame(
        [
            max(slice_.to_dict(orient="records"), key=lambda r: r["datetime"])
            for _, slice_ in df.groupby("uuid")
        ]
    )
    df = df[[c for c in df.columns if not c.startswith("_")]]
    df = df.sort_values(by="datetime", ascending=False)
    if df.status.nunique() <= 1:
        logging.warning(f"pop status=\"{df.pop('status').unique()}\"")
    df = df[df.scheduled_date <= datetime.now()]

    if len(scheduled_dates) > 0:
        df = df[df.scheduled_date.isin(scheduled_dates)]
    if head is not None:
        df = df.head(head)

    # pretty print
    df.datetime = df.datetime.apply(lambda dt: dt.strftime("%Y-%m-%d"))
    df.uuid = df.uuid.str.split("-").apply(lambda l: l[0])
    assert len(df) == df.uuid.nunique()
    assert df.when.nunique() == len({s[0] for s in df.when.unique()})
    df.when = df.when.str[0]

    if out_format == "plain":
        df.set_index("uuid", inplace=True)
        df["name"] = df["name"].apply(_gstasks.StringContractor(name_max_length))
        click.echo(df)
        logging.info(df.columns)
    elif out_format == "json":
        df[["scheduled_date", "due"]] = df[["scheduled_date", "due"]].applymap(
            lambda ds: None if pd.isna(ds) else ds.strftime("%Y-%m-%d")
        )
        logging.info(df.dtypes)
        click.echo(json.dumps(df.to_dict(orient="records")))
    elif out_format == "tsv":
        click.echo(df.to_csv(sep="\t"))


if __name__ == "__main__":
    #    if path.isfile(".env"):
    #        logging.warning("loading .env")
    #        load_dotenv()
    list_reg_checkup_tasks_statuses()
