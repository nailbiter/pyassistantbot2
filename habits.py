#!/usr/bin/env python3
"""===============================================================================

        FILE: /Users/nailbiter/for/forpython/forhabits/kostil/habits.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-03-20T15:00:52.054213
    REVISION: ---

==============================================================================="""

import logging
from datetime import datetime

import click
import pandas as pd
from croniter import CroniterBadCronError, croniter

from _common import TIME_CATS, get_coll, to_utc_datetime


@click.group()
@click.option("--mongo-pass", envvar="MONGO_PASS", required=True)
@click.pass_context
def habits(ctx, **kwargs):
    ctx.ensure_object(dict)
    for k, v in kwargs.items():
        ctx.obj[k] = v


@habits.command()
@click.pass_context
@click.option("--head", type=int, default=5)
def next_habit(ctx, head):
    coll = get_coll(ctx.obj["mongo_pass"], "alex.habits")
    habits_df = pd.DataFrame(coll.find({"enabled": True}))
    now_ = datetime.now()
    habits_df["next_date"] = habits_df["cronline"].apply(
        lambda cl: croniter(cl, now_).get_next(datetime)
    )
    habits_df = habits_df.loc[:, "name cronline onFailed next_date".split(" ")]
    habits_df = habits_df.sort_values(by="next_date")
    click.echo(habits_df.head(head))


def _validate_cronline(ctx, param, value):
    now_ = datetime.now()
    try:
        nr = croniter(value, now_).get_next(datetime)
        logging.warning(
            f"next run: {nr.strftime('%Y-%m-%d %H:%M')} (in {str(nr-now_)})"
        )
    except CroniterBadCronError as e:
        raise click.BadParameter(e)
    return value


@habits.command()
@click.option("-c", "--cronline", callback=_validate_cronline, required=True)
@click.option("-m", "--delaymin", type=click.IntRange(min=1), required=True)
@click.option("-i", "--info")
@click.option("-n", "--name", required=True)
@click.option(
    "-f", "--on-failed", "onfailed", type=click.Choice("move:todo remove".split())
)
@click.option("-s", "--start-date", "start_date", type=click.DateTime())
@click.option("-a", "--category", type=click.Choice(TIME_CATS))
@click.option("--dry-run/--no-dry-run", default=False)
@click.pass_context
def insert_new_habit(
    ctx,
    dry_run,
    **kwargs,
):
    """
    "cronline": "0 10 * * 1-5",
    "delaymin": 300,
    "enabled": true,
    "info": "https...",
    "name": "morning routines card",
    "onFailed": "move:todo",
    "start_date": ,
    "category": "logistics"
    """
    kwargs["start_date"] = to_utc_datetime(
        datetime.now() if kwargs["start_date"] is None else kwargs["start_date"]
    )

    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    kwargs["enabled"] = True

    logging.warning(kwargs)

    if dry_run:
        logging.warning("dry run")
    else:
        logging.warning("no dry run")
        coll = get_coll(ctx.obj["mongo_pass"], "alex.habits")
        coll.insert_one(kwargs)


if __name__ == "__main__":
    habits()
