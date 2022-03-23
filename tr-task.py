#!/usr/bin/env python3
"""===============================================================================

        FILE: ./forhabits/kostil/task.py

       USAGE: ././forhabits/kostil/task.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-12-14T15:39:19.040676
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
from _common import run_trello_cmd, get_coll, to_utc_datetime
import json
import pandas as pd
from datetime import datetime, timedelta
from jinja2 import Template
import itertools
import tqdm

_SEP_TEXT = "------------------------------"


@click.group()
@click.option("--todo-trello-list-id", default=os.environ["TODO_TRELLO_LIST_ID"])
@click.option("--todo-trello-board-id", default=os.environ["TODO_BOARD_ID"])
@click.option("--mongo-pass", envvar="MONGO_PASS")
@click.option("--debug/--no-debug", default=False)
@click.pass_context
def task(ctx, todo_trello_list_id, mongo_pass, todo_trello_board_id, debug):
    if debug:
        logging.basicConfig(level=logging.INFO)
    ctx.ensure_object(dict)
    ctx.obj["trello_list_id"] = todo_trello_list_id
    ctx.obj["trello_board_id"] = todo_trello_board_id
    ctx.obj["mongo_pass"] = mongo_pass


def _get_labels(ctx):
    data = run_trello_cmd(
        f"low get-labels-of-board {ctx.obj['trello_board_id']}")
    data = json.loads(data)
    labels_df = pd.DataFrame(data)
    labels_df = labels_df.set_index(["id"])[["name"]]
    return labels_df


@task.command(name="n")
@click.argument("name")
@click.option("-t", "--tag", multiple=True)
@click.option("--open-url/--no-open-url", default=False)
@click.pass_context
def new_(ctx, tag, name, open_url):
    labels_df = _get_labels(ctx)
    labels_to_use = {k: set(
        slice_.id) for k, slice_ in labels_df.reset_index().groupby("name") if k in tag}

    data = run_trello_cmd(
        f"low get-cards-of-list {ctx.obj['trello_list_id']} --include-assistantbot-hash")
    data = json.loads(data)
    df = pd.DataFrame(data)

    sep_idx = [i for i, n in enumerate(df.name) if n == _SEP_TEXT][0]
    new_pos = df.iloc[sep_idx:sep_idx+2].pos.mean()

    card_url = run_trello_cmd(Template("""high create-card -n "{{name}}" {%for l in labels_to_use%}-l {{l}} {%endfor%} {{"--open-url" if open_url}} --pos {{pos}} --list-id {{trello_list_id}}""").render({
        "name": name,
        "labels_to_use": list(map(min, labels_to_use.values())),
        "open_url": open_url,
        "pos": new_pos,
        "trello_list_id": ctx.obj["trello_list_id"],
    }))
    click.echo(card_url)


@task.command(name="m")
@click.argument("assistantbot_hash", nargs=-1)
@click.option("-t", "--tag", multiple=True)
@click.option("-r", "--repeat-count", type=int, default=1)
@click.option("-d", "--done", is_flag=True, default=False)
@click.option("-a", "--archive", is_flag=True, default=False)
@click.pass_context
def modify(ctx, assistantbot_hash, tag, repeat_count, done, archive):
    labels_df = _get_labels(ctx)
    labels_to_use = {k: set(
        slice_.id) for k, slice_ in labels_df.reset_index().groupby("name") if k in tag}

    for ah, _ in tqdm.tqdm(list(itertools.product(assistantbot_hash, range(repeat_count)))):
        df = _get_tasks(ctx, labels_df=labels_df, reduce_cols=False)
        task = [r for r in df.to_dict(
            orient="records") if r["assistantbot_hash"] == ah][0]

        d = dict(task)
        if pd.isna(d["due"]):
            d["due"] = None
        d = json.loads(json.dumps(d))

        mongo_coll = get_coll(ctx.obj["mongo_pass"], "alex.taskLog")

        for t in (tag):
            is_set = t not in task["labels"]
            print(
                f"{d['name']}: {t} => {is_set} ({datetime.now().strftime('%H:%M')})")
            run_trello_cmd(
                f"low {'add-label-to-card' if is_set else 'remove-label-from-card'} -c {task['id']} -l {min(labels_to_use[t])}")
            if t == "engage":
                mongo_coll.insert_one({"message": (
                    "add engage" if is_set else "rm engage"), "date": to_utc_datetime(), "obj": d})

        if archive:
            print(f"archive task \"{d['name']}\"")
            run_trello_cmd(f"low update-card {task['id']} --closed true")
        if done:
            print(f"done task \"{d['name']}\"")
            mongo_coll.insert_one(
                {"message": "taskdone", "date": to_utc_datetime(), "obj": d})


def _get_tasks(ctx, labels_to_use={}, labels_df=None, reduce_cols=True):
    data = run_trello_cmd(
        f"low get-cards-of-list {ctx.obj['trello_list_id']} --include-assistantbot-hash")
    data = json.loads(data)
    df = pd.DataFrame(data)

    sep_idx = [i for i, n in enumerate(df.name) if n == _SEP_TEXT][0]
    df = df.iloc[sep_idx:]

    for lbls in labels_to_use.values():
        df = df[[len(set(l) & lbls) > 0 for l in df.idLabels]]

    if labels_df is None:
        labels_df = _get_labels(ctx)
    df["labels"] = df.idLabels.apply(lambda lbls: list(
        labels_df.loc[[lbl for lbl in lbls if lbl in labels_df.index]].name))
    df.due = df.due.apply(
        lambda s: None if s is None else datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ"))
    if reduce_cols:
        df = df[["assistantbot_hash", "name", "labels", "due", "shortUrl"]]
    return df


@task.command(name="s")
@click.option("-t", "--tag", multiple=True)
@click.option("-c", "--cutoff", type=int, default=32, envvar="TASK__SHOW__CUTOFF")
@click.pass_context
def show(ctx, tag, cutoff):
    labels_df = _get_labels(ctx)
    tag = list(tag)
    labels_to_use = {k: set(
        slice_.id) for k, slice_ in labels_df.reset_index().groupby("name") if k in tag}

    df = _get_tasks(ctx, labels_to_use, labels_df=labels_df)

    if cutoff >= 0:
        print(df[:cutoff].to_string())
        if len(df) > cutoff:
            print("...")
    else:
        print(df.to_string())
    print(len(df))


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    task()
