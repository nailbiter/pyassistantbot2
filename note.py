#!/usr/bin/env python3
"""===============================================================================

        FILE: /Users/nailbiter/for/forpython/forhabits/kostil/note.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-04-11T16:29:49.905392
    REVISION: ---

==============================================================================="""

import click
import _common
from datetime import datetime, timedelta
import pandas as pd
import pymongo
import seaborn as sns
import matplotlib.pyplot as plt


@click.group()
@click.option("--mongo-pass", envvar="MONGO_PASS", required=True)
@click.pass_context
def note(ctx, mongo_pass):
    ctx.ensure_object(dict)
    ctx.obj["coll"] = _common.get_coll(mongo_pass, "alex.notes")


@note.command()
@click.option("-l", "--limit", type=int, default=10)
@click.option("-r", "--regex")
@click.pass_context
def show(ctx, regex, limit):
    coll = ctx.obj["coll"]
    filter_ = {}
    if regex is not None:
        filter_["content"] = {"$regex": regex}
    df = pd.DataFrame(
        coll.find(filter=filter_, sort=[("date", pymongo.DESCENDING)], limit=limit))
#    df.date = df.date.apply(
#        lambda dt: _common.to_utc_datetime(dt, inverse=True))
    df = df.drop(columns=["_id"])
    click.echo(df)


@note.command()
@click.option("-l", "--limit", type=int, default=20)
@click.option("--show-graph/--no-show-graph", default=True)
@click.pass_context
def show_weight(ctx, limit, show_graph):
    coll = ctx.obj["coll"]
    df = pd.DataFrame(
        coll.find(filter={"content": {"$regex": "#weight"}}, sort=[("date", pymongo.DESCENDING)], limit=limit))
    rs = df.to_dict(orient="records")
    for r in rs:
        content = r["content"]
        split = content.split(" ")
        assert len(split) == 2
        split = [x for x in split if x != "#weight"]
        assert len(split) == 1
        r["weight"] = float(split[0])
    df = pd.DataFrame(rs).drop(columns=["content", "_id"])
    click.echo(pd.DataFrame(
        {**df, "date": df.date.apply(lambda d: d.strftime("%Y-%m-%d(%a) %H:%M"))}))
    if show_graph:
        #        plt.subplots(figsize=(10,10))
        sns.lineplot(data=df, x="date", y="weight")
        plt.show()


@note.command()
@click.option("-x", "--text")
@click.option("-d", "--day", type=click.DateTime(["%Y-%m-%d %H:%M"]))
@click.option("-s", "--days-shift", type=int, default=0)
@click.option("--dry-run/--no-dry-run", default=False)
@click.option("-t", "--tags", type=click.Choice("jerk,longwalk,red_light_bicycle,weight,bank,nailbite,mufg,calories_yesterday,weight,porn,talk,masha,salary".split(",")), multiple=True)
@click.pass_context
def add(ctx, text, day, dry_run, days_shift, tags):
    coll = ctx.obj["coll"]
    for tag in tags:
        text = f"#{tag} {text}"
    if day is None:
        day = datetime.now()
    day -= timedelta(days=days_shift)
    r = {"date": _common.to_utc_datetime(day), "content": text}
    click.echo(r)
    if not dry_run:
        coll.insert_one(r)
        click.echo("no dry run")
    else:
        click.echo("dry run")


if __name__ == "__main__":
    note()
