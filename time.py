#!/usr/bin/env python3
"""===============================================================================

        FILE: time_kostil.py

       USAGE: ./time_kostil.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2020-11-14T09:31:41.579546
    REVISION: ---

==============================================================================="""

import click
import pandas as pd
import pymongo
import _common
import logging
from datetime import datetime, timedelta
import numpy as np
import functools
import inspect
import types
from typing import cast
#import logging

_TIME_CATEGORIES = [
    "useless",
    "gym",
    "social",
    "logistics",
    "sleeping",
    "german",
    "parttime",
    "coding",
    "rest",
    "reading",
]

_TD = timedelta(minutes=30)


@click.group()
@click.option("--mongo_pass", envvar="MONGO_PASS", required=True)
@click.option("-l", "--limit", type=int, default=24*2, envvar="TIME_KOSTIL_LIMIT")
@click.option("-d", "--day", type=click.DateTime(["%Y-%m-%d"]))
@click.pass_context
def time_kostil(ctx, **kwargs):
    logging.basicConfig(level=logging.INFO)
    ctx.ensure_object(dict)
    for k, v in kwargs.items():
        ctx.obj[k] = v


def _ctx_obj_to_filter(obj):
    res = {}
    if obj.get("day", None) is not None:
        day = obj["day"]
        dt = datetime(day.year, day.month, day.day)
        # FIXME: this can be done more robustly, without hardcoding
        dt -= timedelta(hours=9)
        res["$and"] = [{"date": {"$gte": dt}}, {
            "date": {"$lt": dt+timedelta(days=1)}}]
    return res


@time_kostil.command()
@click.option("-r", "--remote-filter", type=click.Choice(_TIME_CATEGORIES))
@click.option("-l", "--local-filter", type=click.Choice(_TIME_CATEGORIES))
@click.option("-g", "--grep", type=click.Choice(_TIME_CATEGORIES))
@click.option("--grep-size", type=int, default=1)
@click.option("-i", "--impute", type=click.Choice(_TIME_CATEGORIES))
@click.pass_context
def show(ctx, remote_filter, local_filter, grep, grep_size, impute):
    coll = _common.get_coll(ctx.obj["mongo_pass"], apply_options=False)
    filter_ = _ctx_obj_to_filter(ctx.obj)
    if remote_filter is not None:
        filter_["category"] = remote_filter
    df = pd.DataFrame(
        coll.find(filter=filter_, sort=[("date", pymongo.DESCENDING)], limit=ctx.obj["limit"]))
    df = df[["_id", "date", "category"]]
    # server write date in JST <-- maybe, better to be changed to UTC?
    df.date = df.date-timedelta(hours=9)
    df.date = df.date.apply(functools.partial(
        _common.to_utc_datetime, inverse=True))

#    click.echo(df)
    to_impute = _common.fill_gaps(df.date, _TD)

    if local_filter:
        df = df[[category == local_filter for category in df["category"]]]

    grep_cat, grep_cnt = grep, grep_size
    if grep_cat is None:
        print(df.to_csv())
    else:
        _df = np.array(df.query(f"category=='{grep_cat}'").index)
        if len(_df) > 0:
            print(df[[min(abs(_df-i)) <= grep_cnt for i in df.index]].to_csv())
        else:
            print(f"no category \"{grep_cat}\"!")

    if len(to_impute) > 0:
        to_impute = [_common.to_utc_datetime(dt) for dt in to_impute]
        # FIXME: print consecutive periods
        logging.warning(
            f"{len(to_impute)} dates can be imputed {_common.consecutive_periods(to_impute,_TD)}")
        if impute is None:
            logging.warning(f"use `-i useless` to perform imputation")
        else:
            logging.warning(f"perform imputation with \"{impute}\"")
            res = coll.insert_many(
                [{"date": dt, "category": impute, "telegram_message_id": "imputation"} for dt in to_impute])
            logging.warning(f"exit {res} after imputation")
#            exit(0)


@time_kostil.command()
@click.pass_context
@click.argument("category", type=click.Choice(_TIME_CATEGORIES))
@click.argument("start", type=int)
@click.option("-e", "--endpoint-inclusive", type=int)
def edit(ctx, category, start, endpoint_inclusive):
    #    logger = logging.getLogger("edit")

    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(
        types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    if endpoint_inclusive is None:
        endpoint_inclusive = start
    assert 0 <= start <= endpoint_inclusive
    coll = _common.get_coll(ctx.obj["mongo_pass"])

    limit = ctx.obj["limit"]
    if endpoint_inclusive+1 > limit:
        logger.warning(f"update limit: {limit} --> {endpoint_inclusive+1}")
        limit = endpoint_inclusive+1

    df = pd.DataFrame(
        coll.find(sort=[("date", pymongo.DESCENDING)], limit=limit))
    records = df.to_dict(orient="records")[start:endpoint_inclusive+1]
    logger.info(records)
    for r in records:
        logger.info(f"{r['_id']}: {r['category']} => {category}")
        coll.update_one({"_id": r["_id"]}, {"$set": {"category": category}})


@time_kostil.command()
@click.pass_context
@click.option("-s", "--start", type=click.DateTime())
@click.option("--dry-run/--no-dry-run", default=False)
@click.option("--category", default="useless", type=click.Choice(_TIME_CATEGORIES))
def fix_db(ctx, start, dry_run, category):
    coll = _common.get_coll(ctx.obj["mongo_pass"])
    df = pd.DataFrame(
        coll.find(
            filter={"date": {"$gt": start}},
            sort=[("date", pymongo.DESCENDING)],
        )
    )
    datemin = df.date.min()
    df.date = df.date.apply(
        lambda dt: datemin+timedelta(minutes=((dt-datemin).total_seconds()//(30*60))*30))
    click.echo(df)

    dup_df = df.groupby("date").agg({"_id": len}).query("_id>1")
    dup_df = dup_df.rename(columns={"_id": "cnt"})
    if len(dup_df) > 0:
        logging.error(f"remove duplicates")
        dup_df = dup_df.join(df.set_index("date")).reset_index()
        click.echo(dup_df)
        for _, slice_ in dup_df.groupby("date"):
            for _id in slice_["_id"].iloc[1:]:
                coll.find_one_and_delete({"_id": _id})
        logging.error(f"exit now")
        exit(0)

    df_ = [datemin]
    while df_[-1] < df.date.max():
        df_.append(df_[-1]+timedelta(minutes=30))
    df_ = pd.DataFrame({"date": df_}).set_index("date")

    df = df_.join(df.set_index("date")).sort_index(
        ascending=False).reset_index()

#    click.echo(pd.DataFrame(df))
    for r in df.to_dict(orient="records"):
        if pd.isna(r["_id"]):
            click.echo(f"insert {r['date']} with cat=\"{category}\"")
            if not dry_run:
                coll.insert_one({"category": category, "date": r["date"]})
        elif r["category"] not in _TIME_CATEGORIES:
            click.echo(f"replace {r} with cat=\"{category}\"")
            if not dry_run:
                coll.update_one({"_id": r["_id"]}, {
                                "$set": {"category": category}})
        else:
            pass


if __name__ == "__main__":
    time_kostil()
