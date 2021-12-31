#!/usr/bin/env python3
"""===============================================================================

        FILE: fix_time.py

       USAGE: ./fix_time.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-02-19T14:17:36.925017
    REVISION: ---

==============================================================================="""

import click
from _common import get_remote_mongo_client
import pandas as pd
from pytz import timezone
from bson.codec_options import CodecOptions
from datetime import datetime, timedelta
import re


def _get_coll(mongo_pass):
    client = get_remote_mongo_client(mongo_pass)
    coll = client.logistics["alex.taskLog"].with_options(
        codec_options=CodecOptions(tz_aware=True, tzinfo=timezone('Asia/Tokyo')))
    return coll


@click.command()
@click.option("--mongo-pass", envvar="MONGO_PASS", required=True)
@click.option("-i", "--index", type=int, default=0)
@click.option("-l", "--limit", type=int, default=10)
@click.option("--time")
@click.option("--dry-run/--no-dry-run",default=False)
@click.option("--remove/--no-remove",default=False)
def fix_time(mongo_pass, index, time, limit,dry_run,remove):
    coll = _get_coll(mongo_pass)
    df = pd.DataFrame(coll.find(
        {"message": "add engage"}, sort=[("date", -1)], limit=limit))
    df["name"] = df["obj"].apply(lambda o:o["name"])
    df = df.drop(columns=["obj","message"])
    click.echo(df)

    o = df.to_dict(orient="records")[index]
    click.echo(o)
    if time is not None or remove:
        if time is not None:
            m = re.match(r"((?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) )?(?P<hour>\d{2}):(?P<minute>\d{2})",time)
            assert m is not None, time
            date = datetime(**{
                **{k:getattr(o["date"],k) for k in "year month day".split(" ")},
                **{k:int(m.group(k)) for k in "hour minute year month day".split(" ") if m.group(k) is not None}
            })
            click.echo(f"{o['date']} => {date}")
            if not dry_run:
                coll.update_one({"_id": o["_id"]}, {"$set": {"date":date-timedelta(hours=9)}})
        elif remove:
            click.echo(f"delete {o}")
            if not dry_run:
                coll.delete_one({"_id": o["_id"]})
    if dry_run:
        click.echo("dry_run")


if __name__ == "__main__":
    fix_time()
