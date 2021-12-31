#!/usr/bin/env python3
from pymongo import MongoClient
from datetime import datetime, timedelta
import json
from tqdm import tqdm
import numpy as np
import re
from pandas import DataFrame, concat, isna
import click
import logging
from croniter import croniter
from uuid import uuid4
from pymongo import MongoClient
from subprocess import getoutput
from _common import get_remote_mongo_client


# global const's
_NOT_SHOW_FIELDS = ["_id", "enabled", "delaymin", "info", "checklist"]
_WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
# global var's
# procedures


def _task_predicate(task, date, **kwargs):
    _logger = logging.getLogger("_task_predicate")
    _logger.info(f"date: {date}")
    _logger.info(f"task: {task}")
    if not task["enabled"]:
        return False
    it = croniter(task["cronline"], date-timedelta(days=1))
    _dt = it.get_next(datetime)
    while _dt.date() < date.date():
        _dt = it.get_next(datetime)
    _logger.info(f"{task['name']} => {_dt}")
    if kwargs["only_permanent_habits"]:
        if task["onFailed"] == "remove":
            return False
    return _dt.date() == date.date()


@click.group()
@click.option("--debug", is_flag=True)
def cli(debug=False):
    if debug:
        logging.basicConfig(level=logging.DEBUG)


@cli.command()
@click.argument("task")
@click.option("--dry_run/--no-dry_run", default=False)
@click.option("--do-all/--no-do-all", default=False)
def done(task, dry_run, do_all):
    tasks_df = _get_tasks()
    task_objs = tasks_df[[name == task for name in tasks_df["name"]]].to_dict(
        orient="records")
    if not do_all:
        task_objs = task_objs[:1]
    for task_obj in task_objs:
        obj = {"task_name": task,
               "task_id": task_obj["_id"], "datetime": datetime.now()}
        print(f"done obj: {obj}")
        if not dry_run:
            MongoClient().habits.habits_done.insert_one(obj)


@cli.command()
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]), multiple=True)
@click.option("-o", "--only_permanent_habits", is_flag=True)
@click.option("--mongopass", envvar="MONGO_PASS", required=True)
@click.option("--dry_run/--no-dry_run", default=False)
def add_batch(date, mongopass, dry_run, debug=False, only_permanent_habits=False):
    if len(date) == 0:
        date = (datetime.now(),)
    logging.info(f"mongopass: {mongopass}")
    client = get_remote_mongo_client(mongopass)
    task_data_coll = client.logistics["alex.habits"]

    res = []
    for d in date:
        _debug = set()
        logger = logging.getLogger("cli")
        tasks = [{k: v for k, v in task.items() if k not in _NOT_SHOW_FIELDS}
                 for task in task_data_coll.find() if _task_predicate(task, d, debug=_debug, only_permanent_habits=only_permanent_habits)]

        logger.info(f"\n{DataFrame(tasks)}")
        logger.info(f"d: {d.strftime('%Y-%m-%d')}")
        logger.debug(f"_debug: {_debug}")
        res.append(DataFrame([{"name": t["name"],
                               "creation date": d.strftime('%Y-%m-%d')} for t in tasks]))

    # print(concat(res).to_csv(sep="\t",index=None,header=None))
    res = concat(res)
    print(res)

    if not dry_run:
        MongoClient().habits.habits.insert_many(res.to_dict(orient="records"))


@cli.command()
@click.argument("name")
@click.option("--dry_run/--no-dry_run", default=False)
def add_one(name, dry_run):
    coll = MongoClient().habits.habits
    obj = {"name": name, "creation date": datetime.now().isoformat()}
    print(f"inserting obj {obj}")
    if not dry_run:
        coll.insert_one(obj)


def _get_tasks():
    tasks_df = DataFrame(MongoClient().habits.habits.find()).set_index("_id")
    habits_done_df = DataFrame(
        MongoClient().habits.habits_done.find()).set_index("task_id")
    tasks_df = tasks_df.join(habits_done_df)
    tasks_df = tasks_df[[isna(datetime) for datetime in tasks_df["datetime"]]]
    tasks_df = tasks_df.drop(columns=["_id"]).reset_index()
    return tasks_df


@cli.command()
@click.option("-s","--search")
@click.option("--expand/--no-expand", default=False)
def list(search, expand):
    tasks_df = _get_tasks()
    if search is not None:
        tasks_df = tasks_df[[search in name for name in tasks_df["name"]]]
    if expand:
        print(tasks_df.to_string())
        print(f"count: {len(tasks_df)}")
    else:
        tasks_df = tasks_df.drop(
            columns=["_id", "datetime", "task_name", "creation date"])
        tasks_df["count"] = 1
        df = tasks_df.groupby("name").aggregate({"count": np.sum})

        print(df.to_string())
        print(f"sum: {sum(df['count'])}")


@cli.command()
def list_done():
    print(DataFrame(
        MongoClient().habits.habits_done.find()).sort_values(by="datetime", ascending=False).to_string(index=None))


@cli.command()
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]), multiple=True)
@click.option("--success/--no-success", default=False)
@click.option("--mongopass", envvar="MONGO_PASS", required=True)
@click.option("--dry-run/--no-dry-run", default=False)
def mark_good_day(date, success, mongopass, dry_run):
    if len(date) == 0:
        date = (datetime.now(),)
    client = MongoClient(
        f"mongodb://nailbiter:{mongopass}@ds149672.mlab.com:49672/logistics?retryWrites=false")
    coll = client.logistics["alex.habitspunch"]
    objs = [{"date": (datetime(d.year, d.month, d.day, 23)-timedelta(hours=9)), "name": "good day",
             "status": "SUCCESS" if success else "FAILURE"} for d in date]
    print(f"objs: {objs}")
    if not dry_run:
        coll.insert_many(objs)


@cli.command()
@click.argument("list_id", envvar="TRELLO_LIST_ID")
@click.option("--dry-run/--no-dry-run", default=False)
@click.option("-l", "--limit", type=int, default=-1)
@click.option("-r", "--regex")
def excise_trello(list_id, dry_run, regex, limit):
    logger = logging.getLogger("excise_trello")
    cards = json.loads(
        getoutput(f"~/for/forpython/trello/trello.py low get-cards-of-list {list_id}"))
    if regex is not None:
        cards = [card for card in cards if re.match(
            regex, card["name"]) is not None]
    coll = MongoClient().habits.habits
    original_size = len(cards)
    if limit >= 0:
        cards = cards[:limit]
    for i, card in tqdm(enumerate(cards), total=len(cards)):
        _card = {
            "name": card["name"], "comment": f"trello:{card['id']},url:{card['shortUrl']}"}
        res = json.loads(getoutput(
            f"~/for/forpython/trello/trello.py low get-actions-on-card {card['id']} -f createCard -f copyCard"))
        assert len(res) > 0, (_card, res)
        date_ = sorted([(datetime.fromisoformat(
            res[i]["date"][:-1]) + timedelta(hours=9)) for i in range(len(res))])[0]
        _card["creation date"] = date_.isoformat()
        logging.info(_card)
        if not dry_run:
            coll.insert_one(_card)
            getoutput(
                f"~/for/forpython/trello/trello.py low update-card {card['id']} --closed true")
        cards[i] = _card
    print(DataFrame(cards).to_string())
    print(f"original_size: {original_size}")


# main
if __name__ == "__main__":
    cli()
