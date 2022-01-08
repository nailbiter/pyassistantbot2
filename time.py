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
from _common import get_remote_mongo_client
from pytz import timezone
from bson.codec_options import CodecOptions
import logging
from datetime import datetime, timedelta

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


def _get_coll(mongo_pass):
    client = get_remote_mongo_client(mongo_pass)
    coll = client.logistics["alex.time"].with_options(
        codec_options=CodecOptions(tz_aware=True, tzinfo=timezone('Asia/Tokyo')))
    return coll


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
@click.pass_context
def show(ctx, remote_filter, local_filter):
    coll = _get_coll(ctx.obj["mongo_pass"])
    filter_ = _ctx_obj_to_filter(ctx.obj)
    if remote_filter is not None:
        filter_["category"] = remote_filter
    df = pd.DataFrame(
        coll.find(filter=filter_, sort=[("date", pymongo.DESCENDING)], limit=ctx.obj["limit"]))
    df = df[["_id", "date", "category"]]
    if local_filter:
        df = df[[category == local_filter for category in df["category"]]]
    print(df.to_csv())


@time_kostil.command()
@click.pass_context
@click.argument("category", type=click.Choice(_TIME_CATEGORIES))
@click.argument("start", type=int)
@click.option("-e", "--endpoint-inclusive", type=int)
def edit(ctx, category, start, endpoint_inclusive):
    logger = logging.getLogger("edit")
    if endpoint_inclusive is None:
        endpoint_inclusive = start
    assert 0 <= start <= endpoint_inclusive
    coll = _get_coll(ctx.obj["mongo_pass"])
    df = pd.DataFrame(
        coll.find(sort=[("date", pymongo.DESCENDING)], limit=ctx.obj["limit"]))
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
    coll = _get_coll(ctx.obj["mongo_pass"])
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
