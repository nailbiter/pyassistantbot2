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
import subprocess
import functools
from _ttask_common import ttask as _ttask


@click.command()
@click.option("-i", "--index", type=int, multiple=True)
@click.option("-f", "--from-to", type=(int, int), multiple=True)
@click.option("-l", "--index-file", type=click.Path(allow_dash=True))
@click.option("--mongo-url", envvar="MONGO_URL", required=True)
@click.option("-g", "--gstasks-line")
@click.option("-h", "--head", type=int)
@click.option("--repeat/--no-repeat", default=False)
@click.option("-r", "--grep")
@click.option(
    "-o", "--out-format", type=click.Choice(["json", "plain"]), default="plain"
)
@click.option("--dry-run/--no-dry-run", default=False)
def ttask(
    index,
    mongo_url,
    gstasks_line,
    repeat,
    from_to,
    head,
    grep,
    index_file,
    out_format,
    dry_run,
):
    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    df, coll = _ttask(
        mongo_url, head=head, grep=grep, is_print=not repeat, out_format=out_format
    )

    index = set(index)
    if index_file is not None:
        with click.open_file(index_file) as f:
            index |= set(pd.read_csv(f, names=["index"])["index"].apply(int))
    for a, b in from_to:
        index |= set(range(a, b + 1))
    index = sorted(index)

    if dry_run:
        logging.warning(index)
        exit(0)

    for i in index:
        r = df.loc[i]
        if gstasks_line is not None:
            cmd = f'./gstasks.py add -n "{r.content}" {gstasks_line}'
            logger.warning(f"> {cmd}")
            ec, out = subprocess.getstatusoutput(cmd)
            assert ec == 0, (ec, out)
            click.echo(out)
        coll.update_one(
            {"_id": r._id},
            {
                "$set": {
                    "status": "DONE",
                    "_last_modification": _common.to_utc_datetime(),
                }
            },
        )
        click.echo(f"done {r._id} ({r.content})")
    if repeat:
        _ttask(mongo_url, head=head, grep=grep)


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    ttask()
