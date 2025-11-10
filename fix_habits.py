#!/usr/bin/env python3
"""===============================================================================

        FILE: ./kostil/fix_habits.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-01-06T21:42:41.838573
    REVISION: ---

==============================================================================="""

import click
from _common import get_remote_mongo_client, to_utc_datetime, get_coll
import pandas as pd
from pytz import timezone
from bson.codec_options import CodecOptions
import logging


@click.command()
@click.option("-R", "--regex")
@click.option("--mongo_pass", envvar="MONGO_PASS", required=True)
@click.option("-l", "--limit", type=int, default=10)
@click.option("-i", "--index", multiple=True, type=int)
@click.option("--dry-run/--no-dry-run", " /-F", default=True)
@click.option("--set-success/--no-set-success", default=True)
@click.option("--debug/--no-debug", default=False)
@click.option("-d", "--date", type=click.DateTime())
def fix_habits(regex, mongo_pass, limit, index, dry_run, set_success, debug, date):
    if debug:
        logging.basicConfig(level=logging.INFO)
    assert mongo_pass is not None
    assert limit > 0
    if len(index) == 0:
        index = (0,)
    for index_ in index:
        assert limit > index_ >= 0
        filter_ = {}
        if regex is not None:
            filter_["name"] = {"$regex": regex}

        coll = get_coll(mongo_pass, "alex.habitspunch2")
        df = pd.DataFrame(coll.find(filter_, sort=[("date", -1)], limit=limit))
        print(df.drop(columns=["_id"]))
        o = df.to_dict(orient="records")[index_]
        status = "DONE" if set_success else "FAILED"
        print(o)
        print(f"{o['status']} => {status}")
        if not dry_run:
            set_ = {"status": status, "_last_modification": to_utc_datetime(date=date)}
            coll.update_one(
                {"_id": o["_id"]},
                {
                    "$set": set_,
                },
            )
            print("no dry run")
        else:
            print("dry run")


if __name__ == "__main__":
    fix_habits()
