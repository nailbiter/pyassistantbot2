#!/usr/bin/env python3
"""===============================================================================

        FILE: forhabits/kostil/gstasks.py

       USAGE: ./forhabits/kostil/gstasks.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-08-31T18:48:17.691280
    REVISION: ---

==============================================================================="""

import click
import json
import subprocess
import pandas as pd
import logging
import os
from os import path
from datetime import datetime
#from __future__ import print_function
import pickle
import logging
import re
import random
from _common import parse_cmdline_datetime, run_trello_cmd
import string
from _gstasks import TaskList, CLI_DATETIME, TagProcessor, UuidCacher
import webbrowser
import subprocess
from jinja2 import Template
import inspect
import types
from typing import cast
import tqdm


# If modifying these scopes, delete the file token.google_spreadsheet.pickle.
_SCOPES = [
    #    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
]


@click.group()
@click.option("--debug/--no-debug", default=False)
@click.option("--list-id", envvar="TODO_TRELLO_LIST_ID", required=True)
@click.option("--mongo-url", envvar="PYASSISTANTBOT_MONGO_URL", required=True)
@click.pass_context
def gstasks(ctx, debug, list_id, mongo_url):
    if debug:
        logging.basicConfig(level=logging.DEBUG)

    ctx.ensure_object(dict)
    ctx.obj["task_list"] = TaskList(
        mongo_url=mongo_url, database_name="gstasks", collection_name="tasks")
    ctx.obj["list_id"] = list_id


@gstasks.command()
@click.option("-t", "--tags", envvar="GSTASKS_MV_TAGS")
@click.option("--contains")
@click.option("--not-contains")
@click.pass_context
def mv(ctx, tags, contains, not_contains):
    list_id = ctx.obj["list_id"]
    if tags is not None:
        tags = tags.split(",")
    else:
        tags = []
    out = subprocess.getoutput(
        f"~/for/forpython/trello/trello.py low get-cards-of-list {list_id}")
    cards_df = json.loads(out)
    cards_df = pd.DataFrame(cards_df)
    cards_df.labels = cards_df.labels.apply(
        lambda l: list(map(lambda r: r["name"], l)))

    cards_df = cards_df[[tags <= l for l in cards_df.labels]]
    if contains is not None:
        cards_df = cards_df[[contains in n for n in cards_df.name]]
    if not_contains is not None:
        cards_df = cards_df[[not_contains not in n for n in cards_df.name]]

    tasks_df = pd.DataFrame({
        "name": [f"=HYPERLINK(\"{shortUrl}\",\"{name}\")" for name, shortUrl in zip(cards_df.name, cards_df.shortUrl)],
        "scheduled date": "",
        "status": "",
        "when": "PARTTIME",
        "due": cards_df.due.apply(lambda s: "" if pd.isna(s) else datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y/%m/%d %H:%M:%S"))
    })
    click.echo(tasks_df.to_csv(sep="\t", index=None, header=None))


@gstasks.command()
@click.option("-h", "--task-hash")
@click.option("-w", "--when", type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")))
@click.option("-s", "--scheduled-date")
@click.option("--archive/--no-archive", default=True)
@click.pass_context
def mv_task(ctx, task_hash, when, scheduled_date, archive):
    task_list = ctx.obj["task_list"]
    scheduled_date = parse_cmdline_datetime(scheduled_date)
#    r, _ = task_list.get_task(uuid_text=uuid_text, index=index)
    url = run_trello_cmd(f"assistantbot open-task {task_hash} --no-open-url")
    m = re.match(r"https://trello.com/c/([a-zA-Z0-9]+)/", url)
    assert m is not None
    data = run_trello_cmd(f"low get-card {m.group(1)}")
    data = json.loads(data)
    r = {"name": data["name"], "URL": url}
    if when is not None:
        r["when"] = when
    if scheduled_date is not None:
        r["scheduled_date"] = scheduled_date.strftime("%Y/%m/%d")
    task_list.insert_or_replace_record(r)
    if archive:
        run_trello_cmd(f"assistantbot archive-task {task_hash}")


@gstasks.command()
@click.option("-i", "--index", type=int, multiple=True)
@click.option("-u", "--uuid-text", multiple=True)
@click.option("--web-browser", envvar="WEBBROWSER")
@click.option("--open-url/--no-open-url", default=True)
@click.pass_context
def open_url(ctx, index, uuid_text, web_browser, open_url):
    task_list = ctx.obj["task_list"]
    for _uuid_text, _index in tqdm.tqdm([(x, None) for x in uuid_text]+[(None, x)for x in index]):
        r, _ = task_list.get_task(uuid_text=_uuid_text, index=_index)
        assert r["URL"], r
        if open_url:
            webbrowser.get(web_browser).open(r["URL"])
        else:
            click.echo(r["URL"])


def _fetch_uuid(uuid):
    if re.match(r"-?\d+", uuid) is not None:
        uuids_df = UuidCacher().get_all()
        uuid = uuids_df.uuid.iloc[int(uuid)]
    return uuid


@gstasks.command()
@click.option("-u", "--uuid-text", multiple=True)
@click.option("-i", "--index", type=int, multiple=True)
@click.option("--create-archived/--no-create-archived", default=True)
@click.option("-l", "--label", multiple=True, envvar="GSTASKS__CREATE_CARD__LABEL")
@click.option("--web-browser", envvar="WEBBROWSER")
@click.option("--open-url/--no-open-url", default=False)
@click.pass_context
def create_card(ctx, index, uuid_text, create_archived, label, open_url, web_browser):
    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(
        types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    task_list, list_id = [ctx.obj[k] for k in "task_list,list_id".split(",")]

    for _uuid_text, _index in tqdm.tqdm([(_fetch_uuid(x), None) for x in uuid_text]+[(None, x)for x in index]):
        r, idx = task_list.get_task(uuid_text=_uuid_text, index=_index)
        #FIXME: later
        url_to_add = None
        if r["URL"]:
            url_to_add = r["URL"]
        cmd = Template("""
        high create-card {%for label in labels%}--label {{label}} {%endfor%} --name '{{r.name}}' {%if create_archived%}--create-archived{%else%}--no-create-archived{%endif%} --list-id {{list_id}}
        """).render({
            "labels": label,
            "r": r,
            "create_archived": create_archived,
            "list_id": list_id,
        })
        url = run_trello_cmd(cmd)
        logger.info(f"url: {url}")

        if url_to_add is not None:
            logger.debug(f"url_to_add: {url_to_add}")
            res = run_trello_cmd(
                f"assistantbot add-url-link '{url}' '{url_to_add}'")
            logger.debug(f"res: {res}")
        task_list.insert_or_replace_record({**r, "URL": url}, index=idx)
        if open_url:
            webbrowser.get(web_browser).open(url)


@gstasks.command()
@click.option("-u", "--uuid-text", multiple=True)
@click.option("-i", "--index", type=int, multiple=True)
@click.option("-n", "--name")
@click.option("-t", "--status", type=click.Choice(["DONE", "FAILED", "REGULAR"]))
@click.option("-w", "--when", type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")))
@click.option("-s", "--scheduled-date")
@click.option("--tag", multiple=True)
@click.option("--url")
# FIXME: allow `NONE` for `due` (use more carefully-written version of `parse_cmdline_datetime`)
# FIXME: allow `NONE` for everything else
@click.option("-d", "--due", type=click.DateTime())
@click.pass_context
def edit(ctx, uuid_text, index, **kwargs):
    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(
        types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)
    uuid_text = list(map(_fetch_uuid, uuid_text))

    task_list = ctx.obj["task_list"]
    kwargs["URL"] = kwargs.pop("url")
    _process_tag = TagProcessor(task_list.get_coll("tags"))
    kwargs["tags"] = [_process_tag(tag) for tag in kwargs.pop("tag")]

    _PROCESSORS = {
        "scheduled_date": lambda s: None if s == "NONE" else parse_cmdline_datetime(s),
    }
    _UNSET = "***UNSET***"
    for k, v in _PROCESSORS.items():
        if kwargs[k] is not None:
            if kwargs[k] == "NONE":
                kwargs[k] = _UNSET
            else:
                kwargs[k] = v(kwargs[k])

    for _uuid_text, _index in tqdm.tqdm([(x, None) for x in uuid_text]+[(None, x)for x in index]):
        r, idx = task_list.get_task(uuid_text=_uuid_text, index=_index)
        logger.debug((r, idx))
        for k, v in kwargs.items():
            if v is not None:
                r[k] = None if v == _UNSET else v
        task_list.insert_or_replace_record(r, index=idx)


@gstasks.command()
@click.option("-n", "--name", required=True)
@click.option("-w", "--when", type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")), required=True)
@click.option("-u", "--url")
@click.option("-s", "--scheduled-date", type=CLI_DATETIME)
@click.option("-t", "--status", type=click.Choice(["REGULAR", "DONE"]))
@click.option("--tags", multiple=True)
@click.option("-d", "--due", type=CLI_DATETIME)
@click.pass_context
def add(ctx, name, when, url, scheduled_date, due, status, tags):
    #    scheduled_date = parse_cmdline_datetime(scheduled_date)
    task_list = ctx.obj["task_list"]
    _process_tag = TagProcessor(task_list.get_coll("tags"))
    r = {
        "name": name,
        "URL": url,
        "scheduled_date": scheduled_date,
        "status": status,
        "when": when,
        "due": due,
        "tags": [_process_tag(tag) for tag in tags],
    }
    uuid = task_list.insert_or_replace_record(r)
    UuidCacher().add(uuid, name)


@gstasks.command()
def show_uuid_cache():
    print(UuidCacher().get_all())


@gstasks.command()
def show_tags():
    raise NotImplementedError()


@gstasks.command()
@click.option("-w", "--when", multiple=True, type=click.Choice("WEEKEND,EVENING,PARTTIME,appropriate,all".split(",")))
@click.option("-x", "--text")
@click.option("-b", "--before-date")
@click.option("-a", "--after-date")
@click.option("-u", "--un-scheduled", is_flag=True, default=False)
@click.option("-o", "--out-format", type=click.Choice(["str", "csv"]))
@click.option("-h", "--head", type=int)
@click.option("-s", "--sample", type=int)
@click.option("--name-lenght-limit", type=int, default=50)
@click.option("--tag", "tags", multiple=True)
@click.pass_context
def ls(ctx, when, text, before_date, after_date, un_scheduled, head, out_format, sample, name_lenght_limit, tags):
    task_list = ctx.obj["task_list"]
    df = task_list.get_all_tasks()
    before_date, after_date = map(
        parse_cmdline_datetime, [before_date, after_date])
    _when = set()
    for w in when:
        if w == "appropriate":
            n = datetime.now()
            if n.weekday() in [5, 6]:
                _when.add("WEEKEND")
            elif 10 <= n.hour <= 18:
                _when.add("PARTTIME")
            else:
                _when.add("EVENING")
        elif w == "all":
            _when.update({"WEEKEND", "PARTTIME", "EVENING"})
        else:
            _when.add(w)
    when = _when

    _process_tag = TagProcessor(task_list.get_coll("tags"))
    tags = [_process_tag(tag) for tag in tags]

    df = df.query("status!='DONE' and status!='FAILED'")
    if len(tags) > 0:
        df = df[[set(_tags) == set(tags) for _tags in df.tags]]
    if un_scheduled:
        df = df[[pd.isna(sd) for sd in df.scheduled_date]]
    if len(when) > 0:
        df = df[[w in when for w in df.when]]
    if text is not None:
        df = df[[text in n for n in df.name]]
    if before_date is not None:
        df = df[[sd <= before_date for sd in df.scheduled_date]]
    if after_date is not None:
        df = df[[sd >= after_date for sd in df.scheduled_date]]
    df.tags = df.tags.apply(lambda tags: ", ".join(
        sorted(map(_process_tag.tag_uuid_to_tag_name, tags))))

    df = df.sort_values(by=["status", "due", "when", "uuid"], ascending=[
                        False, True, True, True], kind="stable")
    if head is not None:
        df = df.head(head)
    if sample is not None:
        click.echo(f"{len(df)} tasks initially")
        df = df.sample(n=sample)

    if name_lenght_limit > 0:
        df.name = df.name.apply(lambda s: s if len(
            s) < name_lenght_limit else f"{s[:name_lenght_limit]}...")

    if out_format is None:
        click.echo(df)
    elif out_format == "str":
        click.echo(df.to_string())
    elif out_format == "csv":
        click.echo(df.to_csv())
    click.echo(f"{len(df)} tasks matched")


if __name__ == "__main__":
    gstasks()
