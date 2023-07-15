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

import inspect
import json
import logging
import os
import pickle
import random
import re
import string
import subprocess
import types
import webbrowser
from datetime import datetime
from os import path
from typing import cast
import functools
from dotenv import load_dotenv
from _gstasks.parsers.dates_parser import DatesQueryEvaluator
import click
import pandas as pd
import tqdm
from jinja2 import Template, Environment, FileSystemLoader
from _common import parse_cmdline_datetime, run_trello_cmd, get_random_fn
import time
from _gstasks import CLI_DATETIME, CLI_TIME, TagProcessor, TaskList, UuidCacher
from _gstasks.additional_states import ADDITIONAL_STATES
from _gstasks.html_formatter import format_html, ifnull
import requests
import numpy as np
import uuid
import pymongo

# FIXME: do without global env
LOADED_DOTENV = None

# If modifying these scopes, delete the file token.google_spreadsheet.pickle.
_SCOPES = [
    #    'https://www.googleapis.com/auth/spreadsheets.readonly',
    "https://www.googleapis.com/auth/spreadsheets",
]

click_option_with_envvar_explicit = functools.partial(click.option, show_envvar=True)


# @click.group(chain=True) #cannot do, because have subcommands
@click.group()
@click_option_with_envvar_explicit("--list-id", required=True)
@click_option_with_envvar_explicit("--mongo-url", required=True)
@click_option_with_envvar_explicit(
    "--uuid-cache-db",
    default=path.abspath(path.join(path.dirname(__file__), ".uuid_cache.db")),
)
@click_option_with_envvar_explicit("-d", "--debug")
@click_option_with_envvar_explicit(
    "--template-dir",
    default=path.join(path.dirname(__file__), "_gstasks/templates"),
    type=click.Path(file_okay=False, dir_okay=True, exists=True, readable=True),
)
@click.pass_context
def gstasks(ctx, mongo_url, debug, **kwargs):
    total_level = logging.INFO
    basic_config_kwargs = {"handlers": [], "level": total_level}
    if debug is not None:
        debug_fn = get_random_fn(".log.txt") if (debug == "@random") else debug
        _handler = logging.FileHandler(filename=debug_fn)
        _handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s,%(msecs)d %(levelname)-8s %(name)s [%(filename)s:%(lineno)d] %(message)s",
                datefmt="%Y-%m-%d:%H:%M:%S",
            )
        )
        _handler.setLevel(total_level)
        basic_config_kwargs["handlers"].append(_handler)
    _handler = logging.StreamHandler()
    _handler.setLevel(logging.WARNING)
    basic_config_kwargs["handlers"].append(_handler)
    logging.basicConfig(**basic_config_kwargs)
    if debug is not None:
        logging.warning(f'log saved to "{debug_fn}"')

    logging.warning(f"ADDITIONAL_STATES: {ADDITIONAL_STATES}")
    if LOADED_DOTENV is not None:
        logging.warning(f'loading "{LOADED_DOTENV}"')

    ctx.ensure_object(dict)
    ctx.obj["task_list"] = TaskList(
        mongo_url=mongo_url, database_name="gstasks", collection_name="tasks"
    )
    for k, v in kwargs.items():
        ctx.obj[k] = v


@gstasks.command()
@click_option_with_envvar_explicit("-t", "--tag", "tags")
@click_option_with_envvar_explicit("--contains")
@click_option_with_envvar_explicit("--not-contains")
@click.pass_context
def mv(ctx, tags, contains, not_contains):
    list_id = ctx.obj["list_id"]
    if tags is not None:
        tags = tags.split(",")
    else:
        tags = []
    out = subprocess.getoutput(
        f"~/for/forpython/trello/trello.py low get-cards-of-list {list_id}"
    )
    cards_df = json.loads(out)
    cards_df = pd.DataFrame(cards_df)
    cards_df.labels = cards_df.labels.apply(lambda l: list(map(lambda r: r["name"], l)))

    cards_df = cards_df[[tags <= l for l in cards_df.labels]]
    if contains is not None:
        cards_df = cards_df[[contains in n for n in cards_df.name]]
    if not_contains is not None:
        cards_df = cards_df[[not_contains not in n for n in cards_df.name]]

    tasks_df = pd.DataFrame(
        {
            "name": [
                f'=HYPERLINK("{shortUrl}","{name}")'
                for name, shortUrl in zip(cards_df.name, cards_df.shortUrl)
            ],
            "scheduled date": "",
            "status": "",
            "when": "PARTTIME",
            "due": cards_df.due.apply(
                lambda s: ""
                if pd.isna(s)
                else datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
                    "%Y/%m/%d %H:%M:%S"
                )
            ),
        }
    )
    click.echo(tasks_df.to_csv(sep="\t", index=None, header=None))


@gstasks.command()
@click_option_with_envvar_explicit("-h", "--task-hash")
@click_option_with_envvar_explicit(
    "-w",
    "--when",
    type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")),
)
@click_option_with_envvar_explicit("-s", "--scheduled-date")
@click_option_with_envvar_explicit("--archive/--no-archive", default=True)
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
@click_option_with_envvar_explicit("-i", "--index", type=int, multiple=True)
@click_option_with_envvar_explicit("-u", "--uuid-text", multiple=True)
@click_option_with_envvar_explicit(
    "--web-browser",
)
@click_option_with_envvar_explicit("--open-url/--no-open-url", default=True)
@click.pass_context
def open_url(ctx, index, uuid_text, web_browser, open_url):
    task_list = ctx.obj["task_list"]
    for _uuid_text, _index in tqdm.tqdm(
        [(x, None) for x in uuid_text] + [(None, x) for x in index]
    ):
        r, _ = task_list.get_task(uuid_text=_uuid_text, index=_index)
        assert r["URL"], r
        if open_url:
            webbrowser.get(web_browser).open(r["URL"])
        else:
            click.echo(r["URL"])


def _fetch_uuid(uuid, uuid_cache_db=None):
    m = re.match(r"^(-?\d+)$", uuid)
    if m is not None:
        uuids_df = UuidCacher(uuid_cache_db).get_all()
        uuid = uuids_df.uuid.iloc[int(m.group(1))]
    return uuid


@gstasks.command()
@click_option_with_envvar_explicit("-u", "--uuid-text", multiple=True)
@click_option_with_envvar_explicit("-i", "--index", type=int, multiple=True)
@click_option_with_envvar_explicit(
    "--create-archived/--no-create-archived", default=True
)
@click_option_with_envvar_explicit(
    "-l",
    "--label",
    multiple=True,
)
@click_option_with_envvar_explicit(
    "--web-browser",
)
@click_option_with_envvar_explicit("--open-url/--no-open-url", default=False)
@click.pass_context
def create_card(ctx, index, uuid_text, create_archived, label, open_url, web_browser):
    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    task_list, list_id = [ctx.obj[k] for k in "task_list,list_id".split(",")]

    for _uuid_text, _index in tqdm.tqdm(
        [(_fetch_uuid(x, ctx.obj["uuid_cache_db"]), None) for x in uuid_text]
        + [(None, x) for x in index]
    ):
        r, idx = task_list.get_task(uuid_text=_uuid_text, index=_index)
        # FIXME: later
        url_to_add = None
        if r["URL"]:
            url_to_add = r["URL"]
        cmd = Template(
            """
        high create-card {%for label in labels%}--label {{label}} {%endfor%} --name '{{r.name}}' {%if create_archived%}--create-archived{%else%}--no-create-archived{%endif%} --list-id {{list_id}}
        """
        ).render(
            {
                "labels": label,
                "r": r,
                "create_archived": create_archived,
                "list_id": list_id,
            }
        )
        url = run_trello_cmd(cmd)
        logger.info(f"url: {url}")

        if url_to_add is not None:
            logger.debug(f"url_to_add: {url_to_add}")
            res = run_trello_cmd(f"assistantbot add-url-link '{url}' '{url_to_add}'")
            logger.debug(f"res: {res}")
        task_list.insert_or_replace_record({**r, "URL": url}, index=idx)
        if open_url:
            webbrowser.get(web_browser).open(url)


@gstasks.command()
@click_option_with_envvar_explicit("-u", "--uuid-text", multiple=True)
@click_option_with_envvar_explicit("-i", "--index", type=int, multiple=True)
@click_option_with_envvar_explicit(
    "-f", "--uuid-list-file", type=click.Path(allow_dash=True)
)
@click_option_with_envvar_explicit("-n", "--name")
@click_option_with_envvar_explicit(
    "-t",
    "--status",
    type=click.Choice(["DONE", "FAILED", "REGULAR", *ADDITIONAL_STATES]),
)
@click_option_with_envvar_explicit(
    "-w",
    "--when",
    type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")),
)
@click_option_with_envvar_explicit("-s", "--scheduled-date")
@click_option_with_envvar_explicit("-g", "--tag", "tags", multiple=True)
@click_option_with_envvar_explicit(
    "--tag-operation",
    type=click.Choice(["symmetric_difference", "union", "difference"]),
    default="symmetric_difference",
)
@click_option_with_envvar_explicit("--url", "URL")
# FIXME: allow `NONE` for `due` (use more carefully-written version of `parse_cmdline_datetime`)
# FIXME: allow `NONE` for everything else
@click_option_with_envvar_explicit("-d", "--due")
@click_option_with_envvar_explicit("-a", "--action-comment")
@click_option_with_envvar_explicit("-c", "--comment")
@click_option_with_envvar_explicit(
    "--create-new-tag/--no-create-new-tag", default=False
)
@click_option_with_envvar_explicit("-l", "--label", type=(str, str))
@click_option_with_envvar_explicit("--post-hook")
@click.pass_context
def edit(
    ctx,
    uuid_text,
    index,
    action_comment,
    uuid_list_file,
    tag_operation,
    create_new_tag,
    post_hook,
    **kwargs,
):
    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)
    uuid_text = list(
        map(
            functools.partial(_fetch_uuid, uuid_cache_db=ctx.obj["uuid_cache_db"]),
            uuid_text,
        )
    )

    if uuid_list_file is not None:
        with click.open_file(uuid_list_file) as f:
            l = f.readlines()
        uuid_text += list(filter(lambda x: len(x) > 0, map(lambda s: s.strip(), l)))

    task_list = ctx.obj["task_list"]
    _process_tag = TagProcessor(
        task_list.get_coll("tags"),
        create_new_tag=create_new_tag,
        flag_name="--create-new-tag",
    )

    _PROCESSORS = {
        "scheduled_date": lambda s: None if s == "NONE" else parse_cmdline_datetime(s),
        "due": lambda s: None if s == "NONE" else parse_cmdline_datetime(s),
        "tags": lambda tags: {_process_tag(tag) for tag in tags},
    }
    _UNSET = "***UNSET***"
    for k, v in _PROCESSORS.items():
        if kwargs[k] is not None:
            if kwargs[k] == "NONE":
                kwargs[k] = _UNSET
            else:
                kwargs[k] = v(kwargs[k])

    for _uuid_text, _index in tqdm.tqdm(
        [(x, None) for x in uuid_text] + [(None, x) for x in index]
    ):
        r, idx = task_list.get_task(uuid_text=_uuid_text, index=_index)
        logger.debug((r, idx))
        for k, v in kwargs.items():
            if v is not None:
                if k == "tags":
                    r["tags"] = sorted(
                        getattr(set, tag_operation)(set(r["tags"]), kwargs["tags"])
                    )
                elif k == "label":
                    r["label"] = {
                        **ifnull(r.get("label", {}), {}),
                        v[0]: v[1],
                    }
                else:
                    r[k] = None if v == _UNSET else v
        task_list.insert_or_replace_record(r, index=idx, action_comment=action_comment)

    if post_hook is not None:
        logging.warning(f'executing post_hook "{post_hook}"')
        os.system(post_hook)


@gstasks.command()
@click_option_with_envvar_explicit("-n", "--name", required=True)
@click_option_with_envvar_explicit(
    "-w",
    "--when",
    type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")),
    required=True,
)
@click_option_with_envvar_explicit("-u", "--url", "URL")
@click_option_with_envvar_explicit("-s", "--scheduled-date", type=CLI_DATETIME)
@click_option_with_envvar_explicit(
    "-t", "--status", type=click.Choice(["REGULAR", "DONE", *ADDITIONAL_STATES])
)
@click_option_with_envvar_explicit("-g", "--tag", "tags", multiple=True)
@click_option_with_envvar_explicit("-d", "--due", type=CLI_DATETIME)
@click_option_with_envvar_explicit("-c", "--comment")
@click_option_with_envvar_explicit(
    "--create-new-tag/--no-create-new-tag", default=False
)
@click_option_with_envvar_explicit("--post-hook")
@click.pass_context
def add(ctx, create_new_tag, post_hook, **kwargs):
    task_list = ctx.obj["task_list"]
    _process_tag = TagProcessor(
        task_list.get_coll("tags"),
        create_new_tag=create_new_tag,
        flag_name="--create-new-tag",
    )

    kwargs["tags"] = [_process_tag(tag) for tag in kwargs["tags"]]
    uuid = task_list.insert_or_replace_record({**kwargs})
    UuidCacher(ctx.obj["uuid_cache_db"]).add(uuid, kwargs["name"])

    if post_hook is not None:
        logging.warning(f'executing post_hook "{post_hook}"')
        os.system(post_hook)


@gstasks.command()
@click.pass_context
def show_uuid_cache(ctx):
    print(UuidCacher(ctx.obj["uuid_cache_db"]).get_all())


@gstasks.group()
@click.pass_context
def tags(ctx):
    pass


@tags.command(name="show")
@click.pass_context
def show_tags(ctx):
    task_list = ctx.obj["task_list"]
    tasks_df = task_list.get_all_tasks()
    tasks_df = tasks_df.query("status!='DONE'")
    tasks_df = tasks_df.explode("tags")

    _process_tag = TagProcessor(task_list.get_coll("tags"))
    tags_df = _process_tag.get_all_tags()

    tasks_df = pd.DataFrame({"uuid": tasks_df.tags, "name": tasks_df.name})
    tasks_df = tags_df.set_index("uuid").join(
        tasks_df.set_index("uuid"), lsuffix="_tag", how="outer"
    )
    _TAG_NONE = "NONE"
    assert _TAG_NONE not in list(tags_df.name)
    tasks_df.name_tag = tasks_df.name_tag.fillna(_TAG_NONE)
    tasks_df = tasks_df.groupby("name_tag").count()
    tasks_df = tasks_df.reset_index().drop(columns=["_id"])
    tasks_df["frac (%)"] = tasks_df.name / tasks_df.name.sum() * 100
    tasks_df = tasks_df.sort_values(by="name", ascending=False)
    print(tasks_df)


@tags.command(name="mv")
@click.argument("tag_from")
@click.argument("tag_to")
@click_option_with_envvar_explicit(
    "--remove-tag-from/--no-remove-tag-from", default=False
)
@click.pass_context
def move_tags(ctx, tag_from, tag_to, remove_tag_from):
    task_list = ctx.obj["task_list"]
    tasks_df = task_list.get_all_tasks()
    tasks_df = tasks_df.query("status!='DONE'")
    #    tasks_df = tasks_df.explode("tags")

    _process_tag = TagProcessor(task_list.get_coll("tags"))
    tag_uuid_from, tag_uuid_to = [_process_tag(t) for t in [tag_from, tag_to]]

    print((tag_uuid_from, tag_uuid_to))
    tasks_df = tasks_df[tasks_df.tags.apply(lambda s: tag_uuid_from in s)]
    for uuid_text in tqdm.tqdm(tasks_df.uuid):
        r, idx = task_list.get_task(uuid_text=uuid_text, index=None)
        r["tags"] = set(r["tags"])
        r["tags"] = sorted(
            set(r["tags"])
            - {
                tag_uuid_from,
            }
            | {
                tag_uuid_to,
            }
        )
        task_list.insert_or_replace_record(r, index=idx)
    if remove_tag_from:
        print(_process_tag.remove_tag_by_uuid(tag_uuid_from))


@gstasks.command()
@click_option_with_envvar_explicit(
    "-u", "--uuid-text", required=True, help='`D` means "disengage"'
)
@click_option_with_envvar_explicit("--post-hook")
@click.pass_context
def engage(ctx, uuid_text, post_hook):
    """
    FIXME: re-integrate via labels/tags/flabels(=fuzzy labels); or integrate into `edit`
    """
    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    task_list = ctx.obj["task_list"]

    if uuid_text == "D":
        r = {"uuid": None}
    else:
        uuid_text = _fetch_uuid(uuid_text, uuid_cache_db=ctx.obj["uuid_cache_db"])

        r, _ = task_list.get_task(uuid_text=uuid_text)

    logger.warning(f"engaging {r}")

    # if uuid_list_file is not None:
    #     with open(uuid_list_file) as f:
    #         l = f.readlines()
    #     uuid_text += list(filter(lambda x: len(x) > 0, map(lambda s: s.strip(), l)))

    coll = task_list.get_coll("engage")
    coll.insert_one({"dt": datetime.now(), "task_uuid": r["uuid"]})

    if post_hook is not None:
        logging.warning(f'executing post_hook "{post_hook}"')
        os.system(post_hook)


@gstasks.group()
@click.pass_context
def remind(ctx):
    ctx.obj["jinja_env"] = Environment(
        loader=FileSystemLoader(path.join(ctx.obj["template_dir"], "remind"))
    )


@remind.command(name="add")
@click_option_with_envvar_explicit("-u", "--uuid-text")
# align cmdline's keys with `gstask add`
@click_option_with_envvar_explicit("-n", "--message")
@click_option_with_envvar_explicit("-s", "--remind-datetime", type=CLI_TIME())
@click.pass_context
def add_remind(ctx, uuid_text, remind_datetime, message):
    if remind_datetime is None:
        remind_datetime = datetime.now()
    assert message is not None or uuid_text is not None
    task_list = ctx.obj["task_list"]
    if uuid_text is not None:
        r, _ = task_list.get_task(uuid_text=uuid_text)
    else:
        r = dict(uuid=None)
    coll = task_list.get_coll("remind")
    rem = dict(
        task_uuid=r["uuid"],
        remind_datetime=remind_datetime,
        sweeped_on=None,
        message=message,
        uuid=str(uuid.uuid4()),
    )
    logging.warning(f"inserting rem: {rem}")
    coll.insert_one(rem)


@remind.command(name="ls")
@click.pass_context
@click_option_with_envvar_explicit(
    "-d", "--remind-datetime", type=click.Choice(["before_now", "after_now", "none"])
)
@click_option_with_envvar_explicit(
    "-s", "--sweeped-on", type=click.Choice(["before_now", "after_now", "none"])
)
@click_option_with_envvar_explicit(
    "-o", "--sort-order", type=(str, click.Choice(["asc", "desc"])), multiple=True
)
def ls_remind(ctx, sort_order, **kwargs):
    # remind_datetime: before, after or none
    # sweeped_on: before, after or none
    task_list = ctx.obj["task_list"]
    coll = task_list.get_coll("remind")
    filter_ = {}
    now = datetime.now()
    for k in ["sweeped_on", "remind_datetime"]:
        if kwargs[k] is not None:
            if kwargs[k] == "none":
                v = None
            elif kwargs[k] == "before_now":
                v = {"$lte": now}
            elif kwargs[k] == "after_now":
                v = {"$gte": now}
            filter_[k] = v
    df = pd.DataFrame(coll.find(filter_))
    logging.warning(f"{len(df)} reminds")

    if len(df) > 0 and len(sort_order) > 0:
        kwargs = dict(
            by=[k for k, _ in sort_order],
            ascending=[(a == "asc") for _, a in sort_order],
        )
        logging.warning(f"sort {kwargs}")
        df.sort_values(
            **kwargs,
            inplace=True,
        )

    click.echo(df.to_csv(sep="\t", index=None))


@remind.command(name="mark")
@click.option("-u", "--uuid", "uuids", required=True, multiple=True)
@click.option("-a", "--action-comment", default="manually marked")
@click.pass_context
def mark_remind(ctx, uuids, **kwargs):
    task_list = ctx.obj["task_list"]
    coll = task_list.get_coll("remind")
    for uuid_ in tqdm.tqdm(uuids):
        (r,) = list(coll.find({"uuid": re.compile("^" + uuid_)}))
        logging.warning(r)
        res = coll.update_one(
            {"uuid": r["uuid"]},
            {"$set": {"sweeped_on": datetime.now(), **kwargs}},
        )
        logging.warning(res)


@remind.command(name="sweep")
@click_option_with_envvar_explicit("--dry-run/--no-dry-run", default=False)
@click_option_with_envvar_explicit("-s", "--slack-url")
@click_option_with_envvar_explicit("-i", "--check-interval-minutes", type=int)
@click_option_with_envvar_explicit(
    "-t", "--template-filename", default="sweep_message.jinja.txt"
)
@click_option_with_envvar_explicit("--snap-to-grid/--no-snap-to-grid", default=False)
@click.pass_context
def sweep_remind(
    ctx, dry_run, slack_url, check_interval_minutes, template_filename, snap_to_grid
):
    task_list = ctx.obj["task_list"]
    coll = task_list.get_coll("remind")

    if snap_to_grid and (check_interval_minutes is not None):
        now = datetime.now()
        now_min = datetime.timestamp(now) / 60
        wait_min = check_interval_minutes * np.ceil(now_min / check_interval_minutes)
        wait_dt = datetime.fromtimestamp(60 * wait_min)
        td = wait_dt - now
        logging.warning(
            f"waiting till {wait_dt.strftime('%Y-%m-%d %H:%M:%S')} (for {td})"
        )
        time.sleep(td.total_seconds())

    while True:
        df = pd.DataFrame(coll.find({"sweeped_on": None}))
        now = datetime.now()
        if len(df) > 0:
            df = df[df["remind_datetime"] <= now]
            logging.warning(df)
            if slack_url is not None and len(df) > 0:
                logging.warning(slack_url)
                requests.post(
                    slack_url,
                    json.dumps(
                        {
                            "text": ctx.obj["jinja_env"]
                            .get_template(template_filename)
                            .render(
                                dict(now=now, df=df.drop(columns=["_id", "sweeped_on"]))
                            )
                        }
                    ),
                    headers={"Content-type": "application/json"},
                )

            if not dry_run:
                logging.warning(f"sweep {len(df)} reminds")
                for _id in df["_id"]:
                    # FIXME: use `update_many`
                    coll.update_one({"_id": _id}, {"$set": {"sweeped_on": now}})

        if check_interval_minutes is None:
            break
        else:
            logging.warning(
                f"sleep {check_interval_minutes} minute(s)... ({now.isoformat()} now)"
            )
            time.sleep(check_interval_minutes * 60)


@gstasks.command()
@click_option_with_envvar_explicit(
    "-w",
    "--when",
    multiple=True,
    type=click.Choice("WEEKEND,EVENING,PARTTIME,appropriate,all".split(",")),
)
@click_option_with_envvar_explicit("-x", "--text")
@click_option_with_envvar_explicit("-b", "--before-date")
@click_option_with_envvar_explicit("-a", "--after-date")
@click_option_with_envvar_explicit("-u", "--un-scheduled", is_flag=True, default=False)
@click_option_with_envvar_explicit(
    "-o", "--out-format", type=click.Choice(["str", "csv", "json", "html"])
)
@click_option_with_envvar_explicit("-h", "--head", type=int)
@click_option_with_envvar_explicit("-s", "--sample", type=int)
@click_option_with_envvar_explicit("--name-lenght-limit", type=int, default=50)
@click_option_with_envvar_explicit("-g", "--tag", "tags", multiple=True)
@click_option_with_envvar_explicit(
    "--out-format-config",
    type=click.Path(dir_okay=False, exists=True),
)
@click_option_with_envvar_explicit("-q", "--scheduled-date-query")
@click_option_with_envvar_explicit("--out-file", type=click.Path())
@click.pass_context
def ls(
    ctx,
    when,
    text,
    before_date,
    after_date,
    un_scheduled,
    head,
    out_format,
    sample,
    name_lenght_limit,
    tags,
    out_format_config,
    scheduled_date_query,
    out_file,
):
    task_list = ctx.obj["task_list"]
    df = task_list.get_all_tasks(
        is_post_processing=out_format not in ["html"],
        is_drop_hidden_fields=out_format not in ["html"],
    )
    before_date, after_date = map(parse_cmdline_datetime, [before_date, after_date])
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
        df = df[[set(_tags) >= set(tags) for _tags in df.tags]]

    if scheduled_date_query is not None:
        df = df[df["scheduled_date"].apply(DatesQueryEvaluator(scheduled_date_query))]
    if un_scheduled and len(df) > 0:
        df = df[[pd.isna(sd) for sd in df["scheduled_date"]]]
    if len(when) > 0 and len(df) > 0:
        df = df[df["when"].isin(when)]
    if text is not None and len(df) > 0:
        df = df[[text in n for n in df.name]]
    if before_date is not None and len(df) > 0:
        df = df[[sd <= before_date for sd in df["scheduled_date"]]]
    if after_date is not None and len(df) > 0:
        df = df[[sd >= after_date for sd in df["scheduled_date"]]]

    df["tags"] = df["tags"].apply(
        lambda tags: sorted(map(_process_tag.tag_uuid_to_tag_name, tags))
    )

    if head is not None:
        df = df.head(head)
    if sample is not None:
        click.echo(f"{len(df)} tasks initially")
        df = df.sample(n=sample)

    df = df.sort_values(
        by=["status", "due", "when", "uuid"],
        ascending=[False, True, True, True],
        kind="stable",
    )

    pretty_df = df.copy()
    pretty_df["tags"] = pretty_df["tags"].apply(", ".join)
    pretty_df["tags"] = pretty_df["tags"].apply(lambda s: f'"{s}"')

    if name_lenght_limit > 0:
        pretty_df.name = pretty_df.name.apply(
            lambda s: s if len(s) < name_lenght_limit else f"{s[:name_lenght_limit]}..."
        )

    if out_format is None:
        click.echo(pretty_df)
    elif out_format == "str":
        click.echo(pretty_df.to_string())
    elif out_format == "json":
        click.echo(pretty_df.to_json(orient="records"))
    elif out_format == "csv":
        click.echo(pretty_df.to_csv())
    elif out_format == "html":
        format_html(
            df,
            out_format_config,
            task_list,
            print_callback=click.echo,
            out_file=out_file,
        )
        logging.warning(f"{len(pretty_df)} tasks matched")
    else:
        raise NotImplementedError((out_format,))

    if out_format not in "json html csv".split():
        click.echo(f"{len(pretty_df)} tasks matched")


# FIXME: short group names, long final command names
# or other way round??
@gstasks.group(name="dr")
@click_option_with_envvar_explicit(
    "--dump-dir",
    type=click.Path(file_okay=False, dir_okay=True),
    default=path.join(path.dirname(__file__), ".gstasks_dump"),
)
@click.pass_context
def dump_restore(ctx, **kwargs):
    os.makedirs(kwargs["dump_dir"], exist_ok=True)
    logging.warning(kwargs["dump_dir"])
    for k, v in kwargs.items():
        ctx.obj[k] = v


@dump_restore.command()
@click.pass_context
@click_option_with_envvar_explicit(
    "-r", "--retention", type=(click.Choice(["num", "dur_days"]), int)
)
@click_option_with_envvar_explicit("-u", "--username")
@click_option_with_envvar_explicit("-p", "--password")
@click_option_with_envvar_explicit("--user-password-none-value", default="NONE")
@click_option_with_envvar_explicit("--mongodump-cmd", default="mongodump")
def dump(ctx, retention, mongodump_cmd, user_password_none_value, **user_password):
    """
    adapted from https://gist.github.com/Lh4cKg/939ce683e2876b314a205b3f8c6e8e9d
    """
    for k, v in list(user_password.items()):
        if v == None:
            del user_password[k]
        elif v == user_password_none_value:
            user_password[k] = None
    logging.warning(f"user_password: {user_password}")

    _COLLECTION_NAMES = {
        k: k
        for k in ["actions", "engage", "remind", "tags", "tasks", "regular_checkup"]
    }
    _DUMP_TS_FORMAT = "%Y%m%dT%H%M%S%f"

    mongo_url = ctx.obj["task_list"].mongo_url
    logging.warning(mongo_url)
    parsed_mongo_url = pymongo.uri_parser.parse_uri(mongo_url)
    logging.warning(parsed_mongo_url)

    now = datetime.now()
    dump_dir = path.join(ctx.obj["dump_dir"], f"dump_{now.strftime(_DUMP_TS_FORMAT)}")
    os.makedirs(dump_dir)
    logging.warning(dump_dir)

    cmd = Template(
        """{{mongodump_cmd}}
        {%for k,v in dict(username=username,password=password)|dictsort%}
        {%if v is not none%}--{{k}}='{{v}}'{%endif%}
        {%endfor%}
        --uri='{{mongo_url}}'  --gzip --out='{{dump_dir}}'
        """
    ).render(
        {
            **parsed_mongo_url,
            **user_password,
            "mongodump_cmd": mongodump_cmd,
            "mongo_url": mongo_url,
            "dump_dir": dump_dir,
        }
    )
    cmd = cmd.strip().replace("\n", " ")
    cmd = re.sub(r"\s+", " ", cmd)
    logging.warning(f"> {cmd}")
    ec, out = subprocess.getstatusoutput(cmd)
    assert ec == 0, (ec, cmd, out)

    if retention is not None:
        # TODO: apply retention policy
        dirs = os.listdir(os.ctx["dump_dir"])
        logging.warning(dirs)


@dump_restore.command()
def restore(ctx):
    pass


if __name__ == "__main__":

    env_fns = [
        path.join(path.dirname(path.abspath(__file__)), ".gstasks.env"),
        ".env",
    ]
    for env_fn in env_fns:
        if path.isfile(env_fn):
            LOADED_DOTENV = env_fn
            load_dotenv(dotenv_path=env_fn)
            break

    gstasks(show_default=True, auto_envvar_prefix="GSTASKS")
