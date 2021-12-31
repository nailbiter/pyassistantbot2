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

import click
from _common import get_coll
import pandas as pd
from croniter import croniter
from datetime import datetime


@click.group()
@click.option("--mongo-pass", envvar="MONGO_PASS", required=True)
@click.pass_context
def habits(ctx, **kwargs):
    ctx.ensure_object(dict)
    for k, v in kwargs.items():
        ctx.obj[k] = v


@habits.command()
@click.pass_context
@click.option("--head",type=int,default=5)
def next_habit(ctx,head):
    coll = get_coll(ctx.obj["mongo_pass"], "alex.habits")
    habits_df = pd.DataFrame(coll.find({"enabled": True}))
    now_ = datetime.now()
    habits_df["next_date"] = habits_df["cronline"].apply(
        lambda cl: croniter(cl, now_).get_next(datetime))
    habits_df = habits_df.loc[:,"name cronline onFailed next_date".split(" ")]
    habits_df = habits_df.sort_values(by="next_date")
    click.echo(habits_df.head(head))


if __name__ == "__main__":
    habits()
