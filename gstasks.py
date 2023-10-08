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
import typing
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
from datetime import datetime, timedelta
from os import path
from typing import cast
import functools
from dotenv import load_dotenv
from _gstasks.parsers.dates_parser import DatesQueryEvaluator
from _gstasks.timing import TimeItContext
import click
import pandas as pd
import tqdm
from jinja2 import Template, Environment, FileSystemLoader
from _common import parse_cmdline_datetime, run_trello_cmd, get_random_fn
import time
from _gstasks import (
    CLI_DATETIME,
    CLI_TIME,
    TagProcessor,
    TaskList,
    UuidCacher,
    CLICK_DEFAULT_VALUES,
    ssj,
    dynamic_wait,
    cmdline_keys_to_sort_kwargs,
    is_sweep_demon_running,
    dump_demon_pid,
)
from _gstasks.additional_states import ADDITIONAL_STATES
import json5
from _gstasks.html_formatter import format_html, ifnull, get_last_engaged_task_uuid
import requests
import numpy as np
import uuid
import pymongo
from alex_leontiev_toolbox_python.utils.click_format_dataframe import (
    AVAILABLE_OUT_FORMATS,
    format_df,
    build_click_options,
    apply_click_options,
)
import operator
import copy

# FIXME: do without global env
LOADED_DOTENV = None

# If modifying these scopes, delete the file token.google_spreadsheet.pickle.
_SCOPES = [
    #    'https://www.googleapis.com/auth/spreadsheets.readonly',
    "https://www.googleapis.com/auth/spreadsheets",
]

moption = functools.partial(click.option, show_envvar=True)


# @click.group(chain=True) #cannot do, because have subcommands
@click.group()
@moption("--list-id", required=True)
@moption("--mongo-url", required=True)
@moption(
    "--uuid-cache-db",
    default=path.abspath(path.join(path.dirname(__file__), ".uuid_cache.db")),
)
@moption("-d", "--debug")
@moption(
    "--template-dir",
    default=path.join(path.dirname(__file__), "_gstasks/templates"),
    type=click.Path(file_okay=False, dir_okay=True, exists=True, readable=True),
)
@moption("--post-hook", type=click.Path())
@click.pass_context
def gstasks(ctx, mongo_url, post_hook, debug, **kwargs):
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


@gstasks.result_callback()
@click.pass_context
def gstasks_result_callback(ctx, _, **kwargs):
    # logging.warning((args, kwargs))

    post_hook = kwargs["post_hook"]
    if post_hook is not None:
        with open(post_hook) as f:
            post_hook = json5.load(f)
        return globals()[post_hook["callback"]](
            ctx=ctx, **{**CLICK_DEFAULT_VALUES["mark"], **post_hook.get("kwargs", {})}
        )


@gstasks.command()
@moption("-t", "--tag", "tags")
@moption("--contains")
@moption("--not-contains")
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
@moption("-h", "--task-hash")
@moption(
    "-w",
    "--when",
    type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")),
)
@moption("-s", "--scheduled-date")
@moption("--archive/--no-archive", default=True)
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
@moption("-i", "--index", type=int, multiple=True)
@moption("-u", "--uuid-text", multiple=True)
@moption(
    "--web-browser",
)
@moption("--open-url/--no-open-url", default=True)
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
@moption("-u", "--uuid-text", multiple=True)
@moption("-i", "--index", type=int, multiple=True)
@moption("--create-archived/--no-create-archived", default=True)
@moption(
    "-l",
    "--label",
    multiple=True,
)
@moption(
    "--web-browser",
)
@moption("--open-url/--no-open-url", default=False)
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


_NONE_CLICK_VALUE = "NONE"


@gstasks.command()
@moption("-u", "--uuid-text", multiple=True)
@moption("-i", "--index", type=int, multiple=True)
@moption("-f", "--uuid-list-file", type=click.Path(allow_dash=True))
@moption("-n", "--name")
@moption(
    "-t",
    "--status",
    type=click.Choice(["DONE", "FAILED", "REGULAR", *ADDITIONAL_STATES]),
)
@moption(
    "-w",
    "--when",
    type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")),
)
@moption("-s", "--scheduled-date")
@moption("-g", "--tag", "tags", multiple=True)
@moption(
    "--tag-operation",
    type=click.Choice(["symmetric_difference", "union", "difference"]),
    default="symmetric_difference",
)
@moption("--url", "URL")
# FIXME: allow `NONE` for `due` (use more carefully-written version of `parse_cmdline_datetime`)
# FIXME: allow `NONE` for everything else
@moption("-d", "--due")
@moption("-a", "--action-comment")
@moption("-c", "--comment")
@moption("--create-new-tag/--no-create-new-tag", default=False)
@moption("-l", "--label", type=(str, str), multiple=True)
@moption("--string-set-mode", type=click.Choice(["set", "rappend"]), default="set")
@moption("--post-hook")
@click.pass_context
def edit(
    ctx,
    uuid_text,
    index,
    action_comment,
    uuid_list_file,
    tag_operation,
    create_new_tag,
    string_set_mode,
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
        "scheduled_date": lambda s: None
        if s == _NONE_CLICK_VALUE
        else parse_cmdline_datetime(s),
        "due": lambda s: None if s == _NONE_CLICK_VALUE else parse_cmdline_datetime(s),
        "tags": lambda tags: {_process_tag(tag) for tag in tags},
    }
    _UNSET = "***UNSET***"
    for k, v in _PROCESSORS.items():
        if kwargs[k] is not None:
            if kwargs[k] == _NONE_CLICK_VALUE:
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
                elif k in ["name", "comment"]:
                    if v == _UNSET:
                        r[k] = None
                    elif string_set_mode == "set":
                        r[k] = v
                    elif string_set_mode == "rappend":
                        r[k] += v
                    else:
                        raise NotImplementedError(dict(string_set_mode=string_set_mode))
                    # r[k] = None if v == _UNSET else v
                elif k == "label":
                    r["label"] = {
                        **ifnull(r.get("label", {}), {}),
                        **{kk: vv for kk, vv in v},
                    }
                else:
                    r[k] = None if v == _UNSET else v
        task_list.insert_or_replace_record(r, index=idx, action_comment=action_comment)

    if post_hook is not None:
        logging.warning(f'executing post_hook "{post_hook}"')
        os.system(post_hook)


@gstasks.command()
@moption("-u", "--uuid-text", "uuid_texts", required=True, multiple=True)
@moption("-s", "--scheduled-date", type=CLI_DATETIME)
@moption("--done/--no-done", "-d/ ", "is_done", default=False)
@click.pass_context
def cp(ctx, uuid_texts, scheduled_date, is_done):
    """
    FIXME:
    1(done). copy and fixup (same as in `edit`)
    2. copy and `done`
    """

    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    task_list = ctx.obj["task_list"]

    uuid_texts = list(
        map(
            functools.partial(_fetch_uuid, uuid_cache_db=ctx.obj["uuid_cache_db"]),
            uuid_texts,
        )
    )
    for uuid_text in tqdm.tqdm(uuid_texts):
        r, _ = task_list.get_task(uuid_text=uuid_text)
        new_r = copy.deepcopy(r)
        _uuid = new_r.pop("uuid")
        new_r["label"] = {**ifnull(r.get("label", {}), {}), "cloned_from": _uuid}
        if scheduled_date is not None:
            new_r["scheduled_date"] = scheduled_date
        logger.warning(f"r:\n{pd.Series(r)}")
        logger.warning(f"new_r:\n{pd.Series(new_r)}")

        if is_done:
            r["status"] = "DONE"
            task_list.insert_or_replace_record(r)
        task_list.insert_or_replace_record(
            new_r, action_comment=f"cloned from '{_uuid}'"
        )


@gstasks.command()
@moption("-n", "--name", "names", multiple=True)
@moption("--names-batch-file", "-f", type=click.Path(allow_dash=True))
@moption(
    "-w",
    "--when",
    type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")),
    required=True,
)
@moption("-u", "--url", "URL")
@moption("-s", "--scheduled-date", type=CLI_DATETIME)
@moption("-t", "--status", type=click.Choice(["REGULAR", "DONE", *ADDITIONAL_STATES]))
@moption("-g", "--tag", "tags", multiple=True)
@moption("-d", "--due", type=CLI_DATETIME)
@moption("-c", "--comment")
@moption("--create-new-tag/--no-create-new-tag", default=False)
@moption("-l", "--label", type=(str, str), multiple=True)
@moption("--post-hook")
@moption("--dry-run/--no-dry-run", default=False)
@click.pass_context
def add(ctx, create_new_tag, names_batch_file, post_hook, names, dry_run, **kwargs):
    names = list(names)
    if names_batch_file is not None:
        with click.open_file(names_batch_file) as f:
            lines = f.readlines()
        lines = [line.strip() for line in lines if len(line.strip()) > 0]
        names.extend(lines)
    logging.warning(names)
    assert len(names) > 0

    task_list = ctx.obj["task_list"]
    _process_tag = TagProcessor(
        task_list.get_coll("tags"),
        create_new_tag=create_new_tag,
        flag_name="--create-new-tag",
    )

    kwargs["tags"] = [_process_tag(tag) for tag in kwargs["tags"]]
    kwargs["label"] = {k: v for k, v in kwargs["label"]}
    for name in tqdm.tqdm(names):
        assert name is not None
        kwargs["name"] = name
        uuid = task_list.insert_or_replace_record(
            copy.deepcopy(kwargs), dry_run=dry_run
        )
        if not dry_run:
            UuidCacher(ctx.obj["uuid_cache_db"]).add(uuid, name)

    if (post_hook is not None) and (not dry_run):
        logging.warning(f'executing post_hook "{post_hook}"')
        os.system(post_hook)


@gstasks.command()
@moption("--json-file-name", "-f", type=click.Path(allow_dash=True))
@moption("-g", "--tag", "tags", multiple=True)
@moption("--create-new-tag/--no-create-new-tag", default=False)
@moption("--dry-run/--no-dry-run", default=False)
@moption("-s", "--scheduled-date", type=CLI_DATETIME)
@click.pass_context
def import_file(ctx, json_file_name, create_new_tag, dry_run, **kwargs):
    task_list = ctx.obj["task_list"]
    _process_tag = TagProcessor(
        task_list.get_coll("tags"),
        create_new_tag=create_new_tag,
        flag_name="--create-new-tag",
    )

    kwargs["tags"] = [_process_tag(tag) for tag in kwargs["tags"]]

    with click.open_file(json_file_name) as f:
        # df = pd.read_json(f)
        df = pd.DataFrame([{**r, **kwargs} for r in json.load(f)])

    for k in ["due", "URL"]:
        if k not in df.columns:
            df[k] = None

    logging.warning(df)

    for kwargs in tqdm.tqdm(df.to_dict(orient="records")):
        name = kwargs["name"]
        assert name is not None
        uuid = task_list.insert_or_replace_record(
            copy.deepcopy(kwargs), dry_run=dry_run
        )
        if not dry_run:
            UuidCacher(ctx.obj["uuid_cache_db"]).add(uuid, name)


@gstasks.command()
@click.pass_context
def show_uuid_cache(ctx):
    print(UuidCacher(ctx.obj["uuid_cache_db"]).get_all())


@gstasks.group()
@click.pass_context
def tags(ctx):
    pass


_TAG_NONE = "NONE"


@tags.command(name="ls")
@moption("-t", "--sort-order", type=(str, click.Choice(["asc", "desc"])), multiple=True)
@moption("--raw/--no-raw", "-r/ ", default=False)
@moption("-o", "--out-format", type=click.Choice(AVAILABLE_OUT_FORMATS))
@click.pass_context
def ls_tags(ctx, sort_order, raw, out_format):
    task_list = ctx.obj["task_list"]
    tasks_df = task_list.get_all_tasks()
    tasks_df = tasks_df.query("status!='DONE'")
    tasks_df = tasks_df.explode("tags")

    _process_tag = TagProcessor(task_list.get_coll("tags"))
    tags_df = _process_tag.get_all_tags()

    if raw:
        df = tags_df
    else:
        tasks_df = pd.DataFrame({"uuid": tasks_df.tags, "name": tasks_df.name})
        tasks_df = tags_df.set_index("uuid").join(
            tasks_df.set_index("uuid"), lsuffix="_tag", how="outer"
        )
        assert _TAG_NONE not in list(tags_df.name)
        tasks_df.name_tag = tasks_df.name_tag.fillna(_TAG_NONE)
        tasks_df = tasks_df.groupby("name_tag").count()
        tasks_df = tasks_df.reset_index().drop(columns=["_id"])
        tasks_df["frac (%)"] = tasks_df.name / tasks_df.name.sum() * 100
        df = tasks_df

    if len(df) > 0:
        if len(sort_order) > 0:
            kwargs = cmdline_keys_to_sort_kwargs(sort_order)
        else:
            kwargs = dict(by=["name"], ascending=[False])
        logging.warning(f"sort {kwargs}")
        df.sort_values(
            **kwargs,
            inplace=True,
        )

    ##FIXME: `json` does not work (ascii-related error)
    click.echo(format_df(df, "plain" if not out_format else out_format))


@gstasks.command(help="ls object")
@click.pass_context
def lso(ctx):
    pass


@tags.command(name="edit")
@moption("-u", "--tag-uuid", required=True)
@click.option("-n", "--name")
@click.option("-c", "--comment")
@click.pass_context
def edit_tags(ctx, tag_uuid, **kwargs):
    task_list = ctx.obj["task_list"]
    tags_coll = task_list.get_coll("tags")
    # help(tags_coll.update_one)
    res = tags_coll.update_one(
        filter={"uuid": tag_uuid},
        update={"$set": {k: v for k, v in kwargs.items() if v is not None}},
    )
    logging.warning(res)


@tags.command(name="add")
@click.pass_context
def add_tags(ctx):
    pass


@tags.command(name="mv")
@click.argument("tag_from")
@click.argument("tag_to")
@moption("--remove-tag-from/--no-remove-tag-from", default=False)
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


_MARK_UNSET_SYMBOL = "D"


@gstasks.group(name="m")
@moption("-m", "--mark", default=CLICK_DEFAULT_VALUES["mark"]["mark"])
@click.pass_context
def mark_group(ctx, mark):
    ctx.obj["mark_group"] = dict(mark=mark)


@mark_group.command(name="ls")
@moption("-f", "--from", "from_", type=click.DateTime())
@moption("-t", "--to", type=click.DateTime())
@moption("--is-use-from/--no-is-use-from", default=True)
@moption("--is-use-to/--no-is-use-to", default=True)
@click.pass_context
def mark_ls(ctx, from_, to, is_use_from, is_use_to):
    """
    TODO:
    1. ls
    2. edit
    3. remove
    """
    mark = ctx.obj["mark_group"]["mark"]
    if from_ is None:
        from_ = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if to is None:
        to = datetime.now()
    logging.warning((from_, to, mark))

    task_list = ctx.obj["task_list"]
    coll = task_list.get_coll("engage")
    filter_ = dict(mark=mark)
    if is_use_to:
        filter_["dt"] = filter_.get("dt", {})
        filter_["dt"]["$lte"] = to
    if is_use_from:
        filter_["dt"] = filter_.get("dt", {})
        filter_["dt"]["$gte"] = from_
    logging.warning(f"f: {filter_}")
    marks_df = pd.DataFrame(coll.find(filter=filter_))
    marks_df.loc[marks_df["task_uuid"].notna(), "task_name"] = (
        marks_df.loc[marks_df["task_uuid"].notna(), "task_uuid"]
        .apply(task_list.get_task)
        .apply(operator.itemgetter(0))
        .apply(operator.itemgetter("name"))
    )
    click.echo(marks_df)


@gstasks.command()
@moption(
    "-u",
    "--uuid-text",
    help=f'`{_MARK_UNSET_SYMBOL}` means "unset"',
)
@moption("--post-hook")
@moption("-m", "--mark", default=CLICK_DEFAULT_VALUES["mark"]["mark"])
@moption("-t", "--time", "time_", type=click.DateTime())
@click.pass_context
def mark(*args, **kwargs):
    """
    FIXME:
      1. re-integrate via labels/tags/flabels(=fuzzy labels); or integrate into `edit`
      2. set up fixed set of marks with fixed arity (=1 by default)
    """
    return real_mark(*args, **kwargs)


def real_mark(
    ctx=None,
    uuid_text: typing.Optional[str] = None,
    post_hook: typing.Optional[str] = None,
    mark: typing.Optional[str] = None,
    time_: typing.Optional[datetime] = None,
):
    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    if time_ is None:
        time_ = datetime.now()

    task_list = ctx.obj["task_list"]

    logger.warning(f'mark: "{mark}"')

    if uuid_text is None:
        task_uuid = get_last_engaged_task_uuid(task_list, mark=mark)
        if task_uuid is None:
            task = task_uuid
        else:
            task, _ = task_list.get_task(uuid_text=task_uuid)

        logger.warning(task)
        echo_kwargs = {}
        if task is None:
            echo_kwargs["bg"] = "red"
        else:
            echo_kwargs["fg"] = "green"
        click.secho(
            Template(
                """currently engaged: {%if task is none%}none{%else%}"{{task.name}}"{%endif%}"""
            ).render(dict(task=task)),
            **echo_kwargs,
        )
    else:
        if uuid_text == _MARK_UNSET_SYMBOL:
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
        coll.insert_one({"dt": time_, "task_uuid": r["uuid"], "mark": mark})

        if post_hook is not None:
            logging.warning(f'executing post_hook "{post_hook}"')
            os.system(post_hook)


@gstasks.group()
@moption("--is-sweep-demon-pid/--no-is-sweep-demon-pid", default=False)
@moption(
    "--sweep-demon-pid-file",
    type=click.Path(),
    default=path.join(path.dirname(__file__), ".gstasks_sweep_demon.pid.json"),
)
@click.pass_context
def remind(ctx, **kwargs):
    ctx.obj["jinja_env"] = Environment(
        loader=FileSystemLoader(path.join(ctx.obj["template_dir"], "remind"))
    )
    for k, v in kwargs.items():
        ctx.obj[k] = v


@remind.command(name="add")
@moption("-u", "--uuid-text")
# align cmdline's keys with `gstask add`
@moption("-n", "--message")
@moption("-s", "--remind-datetime", type=CLI_TIME())
@click.pass_context
def add_remind(ctx, uuid_text, remind_datetime, message):
    if ctx.obj["is_sweep_demon_pid"]:
        is_demon_running, rest = is_sweep_demon_running(ctx.obj)
        logging.warning(
            ssj(
                Template(
                    """
                {%if is_demon_running%}
                sweep demon is running (last check {{datetime.fromisoformat(rest.timestamp_iso).strftime('%Y-%m-%d %H:%M')}} [{{now-datetime.fromisoformat(rest.timestamp_iso)}} ago])
                {%else%}
                sweep demon IS NOT running
                {%endif%}
                """
                ).render(
                    dict(
                        is_demon_running=is_demon_running,
                        rest=rest,
                        datetime=datetime,
                        now=datetime.now(),
                    )
                )
            )
        )

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
    logging.warning(f"inserting rem: {rem} (in {str(remind_datetime-datetime.now())})")
    coll.insert_one(rem)


@remind.command(name="ls")
@moption(
    "-d", "--remind-datetime", type=click.Choice(["before_now", "after_now", "none"])
)
@moption("-s", "--sweeped-on", type=click.Choice(["before_now", "after_now", "none"]))
@moption("-t", "--sort-order", type=(str, click.Choice(["asc", "desc"])), multiple=True)
@moption(
    "-o",
    "--out-format",
)
@click.pass_context
def ls_remind(ctx, sort_order, out_format, **kwargs):
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
        kwargs = cmdline_keys_to_sort_kwargs(sort_order)
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
@moption("--dry-run/--no-dry-run", default=False)
@moption("-s", "--slack-url")
@moption("-i", "--check-interval-minutes", type=int)
@moption("-t", "--template-filename", default="sweep_message.jinja.txt")
@moption(
    "-g",
    "--snap-to-grid",
    type=click.Choice(["none", "static", "dynamic"]),
    default="none",
)
@click.pass_context
def sweep_remind(
    ctx, dry_run, slack_url, check_interval_minutes, template_filename, snap_to_grid
):
    dump_demon_pid(**ctx.obj)

    task_list = ctx.obj["task_list"]
    coll = task_list.get_coll("remind")

    if (snap_to_grid in {"static", "dynamic"}) and (check_interval_minutes is not None):
        wait_dt, td = dynamic_wait(check_interval_minutes)
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
            if snap_to_grid == "dynamic":
                _, td = dynamic_wait(check_interval_minutes)
                sleep_sec = td.total_seconds()
            else:
                sleep_sec = check_interval_minutes * 60

            logging.warning(
                f"sleep {timedelta(seconds=sleep_sec)}... ({now.isoformat()} now)"
            )
            time.sleep(sleep_sec)

        dump_demon_pid(**ctx.obj)


@gstasks.command()
@moption(
    "-w",
    "--when",
    multiple=True,
    type=click.Choice("WEEKEND,EVENING,PARTTIME,appropriate,all".split(",")),
)
@moption("-x", "--text")
@moption("-b", "--before-date")
@moption("-a", "--after-date")
@moption(
    "-u",
    "--un-scheduled",
    is_flag=True,
    default=CLICK_DEFAULT_VALUES["ls"]["un_scheduled"],
)
@moption("-o", "--out-format", type=click.Choice(AVAILABLE_OUT_FORMATS))
@moption("-h", "--head", type=int)
@moption("-s", "--sample", type=int)
@moption(
    "--name-length-limit",
    type=int,
    default=CLICK_DEFAULT_VALUES["ls"]["name_length_limit"],
)
@moption("-g", "--tag", "tags", multiple=True)
@moption(
    "--out-format-config",
    type=click.Path(dir_okay=False, exists=True),
)
@moption("-q", "--scheduled-date-query")
@moption("--out-file", type=click.Path())
@moption("-c", "--column", "columns", type=str, multiple=True)
@moption("-t", "--sort-order", type=(str, click.Choice(["asc", "desc"])), multiple=True)
@moption("--drop-hidden-fields/--no-drop-hidden-fields")
@click.pass_context
def ls(*args, **kwargs):
    real_ls(*args, **kwargs)


def real_ls(
    ctx=None,
    when=CLICK_DEFAULT_VALUES["ls"]["when"],
    text=None,
    before_date=None,
    after_date=None,
    un_scheduled=CLICK_DEFAULT_VALUES["ls"]["un_scheduled"],
    head=None,
    out_format=None,
    sample=None,
    name_length_limit=CLICK_DEFAULT_VALUES["ls"]["name_length_limit"],
    tags=CLICK_DEFAULT_VALUES["ls"]["tags"],
    sort_order=CLICK_DEFAULT_VALUES["ls"]["sort_order"],
    out_format_config=None,
    scheduled_date_query=None,
    out_file=None,
    columns=None,
    drop_hidden_fields: bool = None,
):
    logging.warning(f"dhf: {drop_hidden_fields}")
    timings = {}

    with TimeItContext("prep & tags", report_dict=timings):
        task_list = ctx.obj["task_list"]
        _process_tag = TagProcessor(task_list.get_coll("tags"))
        tags = [_process_tag(tag) for tag in tags]

    with TimeItContext("fetch", report_dict=timings):
        df = task_list.get_all_tasks(
            is_post_processing=out_format not in ["html"],
            is_drop_hidden_fields=(out_format not in ["html"])
            if drop_hidden_fields is None
            else drop_hidden_fields,
            tags=tags,
        )
        logging.warning(f"fetched {len(df)}")
    with TimeItContext("weekend", report_dict=timings):
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

    with TimeItContext("filter (status, tags)", report_dict=timings):
        df = df.query("status!='DONE' and status!='FAILED'")
        if len(tags) > 0:
            df = df[[set(_tags) >= set(tags) for _tags in df.tags]]

    with TimeItContext("filter (DatesQueryEvaluator)", report_dict=timings):
        if scheduled_date_query is not None:
            df = df[
                df["scheduled_date"].apply(DatesQueryEvaluator(scheduled_date_query))
            ]
    with TimeItContext("filter (dates misc)", report_dict=timings):
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

    with TimeItContext("filter (tags)", report_dict=timings):
        ## FIXME takes long time (26s)
        df["tags"] = df["tags"].apply(
            lambda tags: sorted(map(_process_tag.tag_uuid_to_tag_name, tags))
        )

    with TimeItContext("cut & sort", report_dict=timings):
        if head is not None:
            df = df.head(head)
        if sample is not None:
            click.echo(f"{len(df)} tasks initially")
            df = df.sample(n=sample)

        if len(df) > 0:
            if sort_order:
                kwargs = cmdline_keys_to_sort_kwargs(sort_order)
                logging.warning(f"sort {kwargs}")
                df.sort_values(
                    **kwargs,
                    inplace=True,
                    kind="stable",
                )
            else:
                df = df.sort_values(
                    by=["status", "due", "when", "uuid"],
                    ascending=[False, True, True, True],
                    kind="stable",
                )

    with TimeItContext("pretty_df", report_dict=timings):
        pretty_df = df.copy()
        pretty_df["tags"] = pretty_df["tags"].apply(", ".join)
        pretty_df["tags"] = pretty_df["tags"].apply(lambda s: f'"{s}"')

        if name_length_limit > 0:
            pretty_df.name = pretty_df.name.apply(
                lambda s: s
                if len(s) < name_length_limit
                else f"{s[:name_length_limit]}..."
            )

        if columns:
            pretty_df = pretty_df[list(columns)]

    with TimeItContext("format_df", report_dict=timings):
        # if out_format is None:
        #     click.echo(pretty_df)
        # elif out_format == "str":
        #     click.echo(pretty_df.to_string())
        # elif out_format == "json":
        #     click.echo(pretty_df.to_json(orient="records"))
        # elif out_format == "csv":
        #     click.echo(pretty_df.to_csv())
        # elif out_format == "html":
        #     format _html(
        #         df,
        #         out_format_config,
        #         task_list,
        #         print_callback=click.echo,
        #         out_file=out_file,
        #     )
        #     logging.warning(f"{len(pretty_df)} tasks matched")
        # else:
        #     raise NotImplementedError((out_format,))

        click.echo(
            format_df(
                pretty_df,
                "plain" if not out_format else out_format,
                formatters=dict(
                    html=lambda _: format_html(
                        df,
                        out_format_config,
                        task_list,
                        print_callback=click.echo,
                        out_file=out_file,
                    )
                ),
            )
        )

        s = f"{len(pretty_df)} tasks matched"
        if out_format in ["html"]:
            logging.warning(s)
        if out_format not in ["json", "html", "csv", "csvfn"]:
            click.echo(s)

    timings_df = pd.Series(timings).to_frame("duration_seconds")
    timings_df["dur"] = timings_df["duration_seconds"].apply(
        lambda s: timedelta(seconds=s)
    )
    timings_df["perc"] = timings_df["dur"] / timings_df["dur"].sum() * 100
    logging.warning(timings_df)


# FIXME: short group names, long final command names
# or other way round??
@gstasks.group(name="dr")
@moption(
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
@moption("-r", "--retention", type=(click.Choice(["num", "dur_days"]), int))
@moption("-u", "--username")
@moption("-p", "--password")
@moption("--user-password-none-value", default="NONE")
@moption("--mongodump-cmd", default="mongodump")
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

    # _COLLECTION_NAMES = {
    #     k: k
    #     for k in ["actions", "engage", "remind", "tags", "tasks", "regular_checkup"]
    # }

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


@gstasks.group()
@click.pass_context
def analysis(ctx):
    pass


@analysis.command()
@moption("-t", "--target-status", default="DONE")
@moption("--resample/--no-resample", "-r/ ", default=False)
@click.pass_context
def daily_progress(ctx, target_status, resample):
    task_list = ctx.obj["task_list"]
    tasks_df = task_list.get_all_tasks(is_drop_hidden_fields=False)

    actions_df = pd.DataFrame(
        task_list.get_coll("actions").find(
            {"action": "replacing", "r.status": target_status}
        )
    )
    actions_df["uuid"] = actions_df["r"].apply(operator.itemgetter("uuid"))
    # logging.warning(actions_df[["timestamp"]])
    # logging.warning(actions_df)
    done_s = actions_df.groupby(
        actions_df["timestamp"].apply(operator.methodcaller("date"))
    )["uuid"].nunique()

    created_s = (
        tasks_df["_insertion_date"].apply(operator.methodcaller("date")).value_counts()
    )

    df = pd.DataFrame(
        dict(
            created=created_s,
            done=done_s,
        )
    )
    if resample:
        df = pd.DataFrame({k: s.asfreq(freq="D", fill_value=0) for k, s in df.items()})
    df.fillna(0, inplace=True)
    df = df.applymap(int)
    df.sort_index(inplace=True)

    click.echo(df.tail())


@gstasks.group()
@click.pass_context
def relations(ctx):
    pass


@relations.command(name="ls")
@build_click_options
@moption("-t", "--listing-type", type=click.Choice(["relations", "issues"]))
@click.pass_context
def list_relations(ctx, listing_type, **format_df_kwargs):
    raise NotImplementedError()  # 2


@relations.command(name="import")
@moption("-f", "--file-name", type=click.Path(allow_dash=True))
@moption("--pre-clean/--no-pre-clean", "-p/ ", default=False)
@click.pass_context
def import_relations(ctx, file_name, pre_clean):
    raise NotImplementedError()  # 1


@relations.command(name="add")
@moption("-f", "--from", "from_", type=str, required=True)
@moption("-t", "--to", type=str, required=True)
@click.pass_context
def add_relation(ctx, from_, to):
    raise NotImplementedError()  # 3


@relations.command(name="rm")
@moption("-u", "--uuud", "uuid_", type=str, required=True)
@click.pass_context
def delete_relation(ctx, uuid_):
    raise NotImplementedError()  # 4


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
