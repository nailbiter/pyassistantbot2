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
import itertools
import pytz
import typing
import json
import logging
import os
import pickle
import more_itertools
import random
import re
import graphviz
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
from alex_leontiev_toolbox_python.utils import TimeItContext as __TimeItContext__
from _gstasks.jira_helper import (
    JiraHelper,
    generate_symbols_between,
    DEFAULT_JIRA_LABEL,
)
from alex_leontiev_toolbox_python.utils.logging_helpers import (
    get_configured_logger,
    make_log_format,
)
import click
import pandas as pd
import tqdm
from jinja2 import Template, Environment, FileSystemLoader
from _common import parse_cmdline_datetime, run_trello_cmd, get_random_fn
import time
from _gstasks import (
    real_worktime_add,
    real_rolling_log_add,
    process_stopwatch_slice,
    preprocess_stopwatch_slice,
    real_worktime_ls,
    make_mongo_friendly,
    TEMPLATE_DIR_DEFAULT,
    UUID_CACHE_DB_DEFAULT,
    setup_ctx_obj,
    smart_processor,
    CLI_DATETIME,
    CLI_TIME,
    TagProcessor,
    TaskList,
    UuidCacher,
    CLICK_DEFAULT_VALUES,
    ssj,
    dynamic_wait,
    cmdline_keys_to_sort_kwargs,
    is_sweep_daemon_running,
    dump_daemon_pid,
    GSTASK_UUID,
)
from _gstasks.additional_states import ADDITIONAL_STATES
import json5
from _gstasks.html_formatter import format_html, ifnull, get_last_engaged_task_uuid
import requests
import numpy as np
import uuid
import pymongo
from alex_leontiev_toolbox_python.utils.click_helpers.datetime_classes import (
    SimpleCliDatetimeParamType,
)
from alex_leontiev_toolbox_python.utils.click_helpers.format_dataframe import (
    AVAILABLE_OUT_FORMATS,
    format_df,
    build_click_options,
    apply_click_options,
)
import operator
import copy
from _gstasks.habits_backfill import generate_habits_series

## https://stackoverflow.com/a/11875813
from bson import json_util

# FIXME: do without global env
LOADED_DOTENVS = []

STANDARD_STATES = ["DONE", "FAILED", "REGULAR"]

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
@moption("--uuid-cache-db", default=UUID_CACHE_DB_DEFAULT)
@moption("-d", "--debug")
@moption(
    "--template-dir",
    default=TEMPLATE_DIR_DEFAULT,
    type=click.Path(file_okay=False, dir_okay=True, exists=True, readable=True),
)
@moption("--post-hook", type=click.Path())
@moption("--labels-types-json5", type=click.Path())
@click.pass_context
def gstasks(ctx, mongo_url, post_hook, debug, labels_types_json5, **kwargs):
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
    logging.warning(f'loaded "{LOADED_DOTENVS}"')

    ctx.ensure_object(dict)
    setup_ctx_obj(
        ctx, labels_types_json5=labels_types_json5, mongo_url=mongo_url, **kwargs
    )


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
    type=click.Choice([*STANDARD_STATES, *ADDITIONAL_STATES]),
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
def edit(ctx, **kwargs):
    return real_edit(ctx, **kwargs)


def real_edit(
    ctx,
    uuid_text: typing.Tuple[str] = tuple(),
    index: list[int] = [],
    action_comment: typing.Optional[str] = None,
    uuid_list_file=None,
    tag_operation: str = "symmetric_difference",  # FIXME: sync with above
    create_new_tag: bool = False,
    string_set_mode: str = "set",  # FIXME: sync with above
    post_hook=None,
    **kwargs,
) -> None:
    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)
    logging.warning(uuid_text)
    uuid_text = list(
        map(
            functools.partial(_fetch_uuid, uuid_cache_db=ctx.obj.get("uuid_cache_db")),
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
@moption("-t", "--status", type=click.Choice([*STANDARD_STATES, *ADDITIONAL_STATES]))
@moption("--done/--no-done", "-d/ ", "is_done", default=False)
@moption("-n", "--edit-name", type=str)
@moption("-E", "--edit-name-mode", type=click.Choice(["set", "replace"]), default="set")
@click.pass_context
def cp(ctx, uuid_texts, scheduled_date, is_done, status, edit_name, edit_name_mode):
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

        if status is not None:
            new_r["status"] = status
        if scheduled_date is not None:
            new_r["scheduled_date"] = scheduled_date
        logging.warning((edit_name, edit_name_mode))
        if edit_name is not None:
            if edit_name_mode == "set":
                new_r["name"] = edit_name
            elif edit_name_mode == "replace":
                a, b = edit_name.split("//")
                logging.warning((a, b))
                new_r["name"] = re.sub(a, b, new_r["name"])
            else:
                raise NotImplementedError(dict(edit_name_mode=edit_name_mode))

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
@moption("-t", "--status", type=click.Choice([*STANDARD_STATES, *ADDITIONAL_STATES]))
@moption("-g", "--tag", "tags", multiple=True)
@moption("-d", "--due", type=CLI_DATETIME)
@moption("-c", "--comment")
@moption("--create-new-tag/--no-create-new-tag", default=False)
@moption("-l", "--label", type=(str, str), multiple=True)
@moption("--post-hook")
@moption("--dry-run/--no-dry-run", default=False)
@click.pass_context
def add(*args, **kwargs):
    return real_add(*args, **kwargs)


def real_add(
    ctx,
    names: list[str],
    create_new_tag: bool = False,
    names_batch_file: typing.Optional[str] = None,
    post_hook: typing.Optional[str] = None,
    dry_run: bool = False,
    **kwargs,
) -> dict:
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

    kwargs["tags"] = [_process_tag(tag) for tag in kwargs.get("tags", [])]

    labels_types = ctx.obj["labels_types"]
    label = {k: v for k, v in kwargs.get("label", [])}
    for k, v in label.items():
        for k in labels_types:
            assert labels_types[k].is_validated(v), (k, labels_types[k], v)
    kwargs["label"] = label

    debug_info = dict(uuids=[])

    for name in tqdm.tqdm(names):
        assert name is not None
        kwargs["name"] = name
        uuid = task_list.insert_or_replace_record(
            copy.deepcopy(kwargs), dry_run=dry_run
        )
        debug_info["uuids"].append(uuid)
        if not dry_run:
            UuidCacher(ctx.obj["uuid_cache_db"]).add(uuid, name)

    if (post_hook is not None) and (not dry_run):
        logging.warning(f'executing post_hook "{post_hook}"')
        os.system(post_hook)

    return debug_info


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
@build_click_options
@click.pass_context
def ls_tags(ctx, sort_order, raw, **format_df_kwargs):
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
    # click.echo(format_df(df, "plain" if not out_format else out_format))
    click.echo(apply_click_options(df, format_df_kwargs))


@gstasks.command(help="ls object")
@moption("-u", "--uuid", "uuids", type=str, multiple=True)
@moption("-f", "--uuid-file", type=click.Path(allow_dash=True))
@moption("-t", "--object-type", type=click.Choice(["task", "tag"]), default="task")
@click.pass_context
def lso(ctx, uuids, object_type, uuid_file):
    """
    output in JSON(L) format
    """
    uuids = list(uuids)
    if uuid_file is not None:
        with click.open_file(uuid_file) as f:
            uuids.extend(f.read().strip().split())
    assert len(uuids) > 0
    return real_lso(ctx, uuids, object_type)


def real_lso(ctx, uuids: list[str], object_type: str, is_loud: bool = True) -> str:
    res = ""
    if object_type == "tag":
        task_list = ctx.obj["task_list"]
        tags_coll = task_list.get_coll("tags")
        for uuid_text in uuids:
            (r,) = list(
                tags_coll.find({"uuid": {"$regex": re.compile("^" + uuid_text)}})
            )
            logging.warning(r)
            s = json.dumps(r, default=json_util.default)
            res += s
            click.echo(s)
    elif object_type == "task":
        task_list = ctx.obj["task_list"]
        for uuid_text in uuids:
            r, _ = task_list.get_task(
                uuid_text=uuid_text,
                index=None,
                get_all_tasks_kwargs=dict(is_drop_hidden_fields=False),
            )

            for k in [
                "scheduled_date",
                "_insertion_date",
                "_last_modification_date",
                "due",
            ]:
                if pd.isna(r[k]):
                    r[k] = None
                else:
                    # r[k] = r[k].tz_localize(pytz.UTC)
                    r[k] = pd.to_datetime(r[k]).to_pydatetime()
                # logging.warning(r[k].tzinfo)
            # r.pop("_id")
            logging.info(r)
            s = json.dumps(r, default=json_util.default)
            res += s
            click.echo(s)
    else:
        raise NotImplementedError(dict(object_type=object_type))

    return res


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


@gstasks.command()
@moption("-t", "--text", required=True)
@moption("-c", "--column-name", default="name")
@moption("--is-apply-lower/--no-is-apply-lower", " /-n", default=True)
@build_click_options
@click.pass_context
def grep(ctx, text, column_name, is_apply_lower, **format_df_kwargs):
    logging.warning(f"is_apply_lower: {is_apply_lower}")
    task_list = ctx.obj["task_list"]
    tasks_df = task_list.get_all_tasks(
        is_post_processing=False, is_drop_hidden_fields=False
    )
    tasks_df = tasks_df[
        tasks_df[column_name]
        .apply(lambda s: (s.lower() if is_apply_lower else s))
        .apply(lambda s: text in s)
    ]
    click.echo(apply_click_options(tasks_df, format_df_kwargs))


MARK_UNSET_SYMBOL = "D"


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
@moption(
    "-s",
    "--set-dt",
    type=(
        str,
        SimpleCliDatetimeParamType(
            formats=[
                "%Y-%m-%d %H:%M",
                "%H:%M",
            ],
            short_dt_types={
                "%H:%M": {"year", "month", "day"},
            },
        ),
    ),
    multiple=True,
)
@moption("-r", "--remove", type=str, multiple=True)
@moption("--groupby/--no-groupby", "-g/ ", default=False)
@build_click_options
@click.pass_context
def mark_ls(
    ctx, from_, to, is_use_from, is_use_to, set_dt, remove, groupby, **format_df_kwargs
):
    """
    TODO:
    1(done). ls
    2. edit
    3. remove
    4. compute stats (groupby)
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
    marks_df.sort_values(by="dt", inplace=True, ignore_index=True)

    if groupby:
        df = marks_df.copy()
        df.drop(columns=["mark", "_id"], inplace=True)
        df["next_dt"] = df["dt"].shift(-1)
        df["next_dt"].iloc[-1] = datetime.now()
        df["next_task"] = df["task_uuid"].shift(-1)
        df["dur"] = df["next_dt"] - df["dt"]
        df = df[["task_uuid", "task_name", "dt", "dur"]]
        df = df.groupby("task_uuid").agg({"task_name": "last", "dur": "sum"})
        df.reset_index(inplace=True)

        logging.warning("\n" + df.to_string())
        exit(0)

    click.echo(
        apply_click_options(marks_df.drop(columns=["_id", "mark"]), format_df_kwargs)
    )

    if set_dt:
        # TODO
        for uuid_, dt in tqdm.tqdm(set_dt):
            res = coll.update_one({"uuid": uuid_}, {"$set": {"dt": dt}})
            logging.info(res)
    if remove:
        for uuid_ in tqdm.tqdm(remove):
            logging.info(coll.delete_one({"uuid": uuid_}))


@gstasks.command()
@moption(
    "-u",
    "--uuid-text",
    help=f'`{MARK_UNSET_SYMBOL}` means "unset"',
)
@moption("--post-hook")
@moption("-m", "--mark", default=CLICK_DEFAULT_VALUES["mark"]["mark"])
@moption("--is-out/--no-is-out", "-o/ ", default=False)
@moption(
    "-t",
    "--time",
    "time_",
    type=SimpleCliDatetimeParamType(
        formats=[
            "%Y-%m-%d %H:%M",
            "%H:%M",
        ],
        short_dt_types={
            "%H:%M": {"year", "month", "day"},
        },
    ),
)
@click.pass_context
def mark(*args, **kwargs):
    """
    FIXME:
      1. re-integrate via labels/tags/flabels(=fuzzy labels); or integrate into `edit`
      2. set up fixed set of marks with fixed arity (=1 by default)
    """
    res, _ = real_mark(*args, **kwargs)
    return res


def real_mark(
    ctx=None,
    uuid_text: typing.Optional[str] = None,
    post_hook: typing.Optional[str] = None,
    is_out: bool = False,
    mark: typing.Optional[str] = None,
    time_: typing.Optional[datetime] = None,
) -> (dict, dict):
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
            err=True,
            **echo_kwargs,
        )
        if is_out:
            click.echo(None if task is None else task["uuid"])
    else:
        if uuid_text == MARK_UNSET_SYMBOL:
            r = {"uuid": None}
        else:
            uuid_text = _fetch_uuid(
                uuid_text, uuid_cache_db=ctx.obj.get("uuid_cache_db")
            )
            r, _ = task_list.get_task(uuid_text=uuid_text)

        logger.warning(f"engaging {r}")

        # if uuid_list_file is not None:
        #     with open(uuid_list_file) as f:
        #         l = f.readlines()
        #     uuid_text += list(filter(lambda x: len(x) > 0, map(lambda s: s.strip(), l)))

        coll = task_list.get_coll("engage")
        r = {
            "dt": time_,
            "task_uuid": r["uuid"],
            "mark": mark,
            "uuid": str(uuid.uuid4()),
            "task_snapshot": make_mongo_friendly(r),
        }
        coll.insert_one(r)

        last_two_df = pd.DataFrame(
            more_itertools.padded(
                coll.find({}, sort=[("dt", pymongo.DESCENDING)], limit=2), {}, 2
            )
        )
        logger.warning("\n" + last_two_df.to_string())

        if post_hook is not None:
            logging.warning(f'executing post_hook "{post_hook}"')
            os.system(post_hook)

        return r, {"last_two_df": last_two_df}


@gstasks.group()
@click.pass_context
def stopwatch(ctx):
    ctx.obj["stopwatch"] = dict(coll=ctx.obj["task_list"].get_coll("stopwatch"))


@stopwatch.command(name="start")
@click.option("-n", "--name", type=str, required=True)
@click.pass_context
def start_stopwatch(ctx, name):
    coll = ctx.obj["stopwatch"]["coll"]
    coll.insert_one(dict(name=name, action="start", now=datetime.now()))


@stopwatch.command(name="remove")
@click.option("-n", "--name", type=str, required=True)
@click.pass_context
def remove_stopwatch(ctx, name):
    coll = ctx.obj["stopwatch"]["coll"]
    coll.delete_(dict(name=name))


@stopwatch.command(name="stop")
@click.option("-n", "--name", type=str, required=True)
@click.pass_context
def stop_stopwatch(ctx, name):
    coll = ctx.obj["stopwatch"]["coll"]
    coll.insert_one(dict(name=name, action="stop", now=datetime.now()))


@stopwatch.command(name="ls")
@moption("-t", "--type", "type_", type=click.Choice(["running", "stopped"]))
@moption("-n", "--stopwatch-name", type=str)
@click.pass_context
def ls_stopwatch(ctx, type_, stopwatch_name):
    coll = ctx.obj["stopwatch"]["coll"]
    now = datetime.now()
    df = pd.DataFrame(coll.find())

    if stopwatch_name is not None:
        df = df[df["name"].eq(stopwatch_name)]
        rs = preprocess_stopwatch_slice(df)
        df = pd.DataFrame(
            [
                {**r, "label": "start" if i % 2 == 0 else "stop", "i": i // 2}
                for i, r in enumerate(rs)
            ]
        )
        df = pd.pivot(df, index="i", columns=["label"])
        df[("now", "dur")] = df["now", "stop"] - df["now", "start"]
    else:
        df = pd.DataFrame(
            [
                dict(process_stopwatch_slice(slice_), name=n)
                for n, slice_ in df.groupby("name")
            ]
        )
        if type_ == "running":
            df = df[df["is_running"]]
        elif type_ == "stopped":
            df = df[~df["is_running"]]
        df.set_index("name", inplace=True)
        df.sort_index(inplace=True)

    click.echo(df)


@gstasks.group()
@moption("--is-sweep-daemon-pid/--no-is-sweep-daemon-pid", default=False)
@moption(
    "--sweep-daemon-pid-file",
    type=click.Path(),
    default=path.join(path.dirname(__file__), ".gstasks_sweep_daemon.pid.json"),
)
@moption(
    "--popup-cmd-tpl",
    type=str,
    default="""osascript -e 'tell app "System Events" to display dialog "{{message}}"'""",
    help="should contain ref to {{message}}",
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
@moption(
    "-m",
    "--media",
    "medias",
    type=click.Choice(["slack", "popup", "file", "voice"]),
    default=("slack",),
    multiple=True,
    help="see https://click.palletsprojects.com/en/8.1.x/options/#multiple-values-from-environment-values",
)
@click.pass_context
def add_remind(ctx, uuid_text, remind_datetime, message, medias):
    medias = list(set(medias))
    logging.warning(f"medias: {medias}")

    if ctx.obj["is_sweep_daemon_pid"]:
        is_daemon_running, rest = is_sweep_daemon_running(ctx.obj)
        echo_kwargs = {}
        if is_daemon_running:
            echo_kwargs["fg"] = "green"
        else:
            echo_kwargs["bg"] = "red"
        click.secho(
            ssj(
                Template(
                    """
                {%if is_daemon_running%}
                sweep daemon is running (last check {{datetime.fromisoformat(rest.timestamp_iso).strftime('%Y-%m-%d %H:%M')}} [{{now-datetime.fromisoformat(rest.timestamp_iso)}} ago])
                {%else%}
                sweep daemon IS NOT running
                {%endif%}
                """
                ).render(
                    dict(
                        is_daemon_running=is_daemon_running,
                        rest=rest,
                        datetime=datetime,
                        now=datetime.now(),
                    )
                )
            ),
            err=True,
            **echo_kwargs,
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
    for media in medias:
        rem = dict(
            task_uuid=r["uuid"],
            remind_datetime=remind_datetime,
            sweeped_on=None,
            message=message,
            media=media,
            uuid=str(uuid.uuid4()),
        )
        logging.warning(
            f"inserting rem: {rem} (in {str(remind_datetime-datetime.now())})"
        )
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
    if len(df) > 0:
        df["until"] = df["remind_datetime"] - datetime.now()
        df.drop(columns=["_id"], inplace=True)

        if len(sort_order) > 0:
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
@moption("-v", "--voice-template", type=str, default='say "you have task"')
@moption("-r", "--reminder-file", type=click.Path())
@moption(
    "-g",
    "--snap-to-grid",
    type=click.Choice(["none", "static", "dynamic"]),
    default="none",
)
@click.pass_context
def sweep_remind(
    ctx,
    dry_run,
    slack_url,
    check_interval_minutes,
    template_filename,
    snap_to_grid,
    reminder_file,
    voice_template,
):
    logging.warning(slack_url)
    dump_daemon_pid(**ctx.obj)

    if reminder_file is None:
        reminder_file = get_random_fn(".txt")
    logging.warning(f"reminder_file: {reminder_file}")

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
        if (len(df) > 0) and (df["remind_datetime"] <= now).any():
            df = df[df["remind_datetime"] <= now]
            logging.warning(df)
            for media, df_ in df.groupby("media"):
                text = (
                    ctx.obj["jinja_env"]
                    .get_template(template_filename)
                    .render(
                        dict(
                            now=now,
                            df=df_.drop(columns=["_id", "sweeped_on"]),
                        )
                    )
                )
                if media == "slack":
                    logging.warning(f"slack_url: {slack_url}")
                    if slack_url is not None:
                        requests.post(
                            slack_url,
                            json.dumps({"text": text}),
                            headers={"Content-type": "application/json"},
                        )
                elif media == "popup":
                    for r in df_.to_dict(orient="records"):
                        cmd = Template(ctx.obj["popup_cmd_tpl"]).render(r)
                        logging.warning(f"> {cmd}")
                        os.system(cmd)
                elif media == "voice":
                    for r in df_.to_dict(orient="records"):
                        cmd = Template(voice_template).render(dict(df=df_))
                        logging.warning(f"> {cmd}")
                        os.system(cmd)
                elif media == "file":
                    with open(reminder_file, "a") as f:
                        f.write(f"{datetime.now().isoformat()} {text}\n\n")
                else:
                    raise NotImplementedError(dict(media=media))

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

        dump_daemon_pid(**ctx.obj)


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
@moption("-G", "--exclude-tag", "exclude_tags", multiple=True)
@moption(
    "--out-format-config",
    type=click.Path(dir_okay=False, exists=True),
)
@moption("-q", "--scheduled-date-query")
@moption("--out-file", type=click.Path())
@moption("-c", "--column", "columns", type=str, multiple=True)
@moption("-t", "--sort-order", type=(str, click.Choice(["asc", "desc"])), multiple=True)
@moption("--drop-hidden-fields/--no-drop-hidden-fields")
@moption("-l", "--label", "labels", multiple=True, type=(str, str))
@moption("-C", "--smart-column", "smart_columns", type=(str, str), multiple=True)
@moption(
    "--filter-out-states",
    type=str,
    default=CLICK_DEFAULT_VALUES["ls"]["filter_out_states"],
)
@click.pass_context
def ls(*args, **kwargs):
    real_ls(*args, **kwargs)


def real_ls(
    ctx=None,
    filter_out_states: str = CLICK_DEFAULT_VALUES["ls"]["filter_out_states"],
    when=CLICK_DEFAULT_VALUES["ls"]["when"],
    smart_columns: list[(str, str)] = CLICK_DEFAULT_VALUES["ls"]["smart_columns"],
    text=None,
    labels=[],
    before_date: typing.Optional[str] = None,
    after_date: typing.Optional[str] = None,
    un_scheduled=CLICK_DEFAULT_VALUES["ls"]["un_scheduled"],
    head=None,
    out_format=None,
    sample=None,
    name_length_limit=CLICK_DEFAULT_VALUES["ls"]["name_length_limit"],
    tags=CLICK_DEFAULT_VALUES["ls"]["tags"],
    exclude_tags=CLICK_DEFAULT_VALUES["ls"]["tags"],
    sort_order=CLICK_DEFAULT_VALUES["ls"]["sort_order"],
    out_format_config=None,
    scheduled_date_query=None,
    relations: typing.Optional[typing.Tuple] = None,
    out_file=None,
    columns=[],
    drop_hidden_fields: bool = None,
    click_echo: typing.Callable = click.echo,
):
    logger = get_configured_logger(
        "real_ls",
        log_format=make_log_format(
            [
                "name",
                "levelname",
                "asctime",
                "message",
            ]
        ),
    )
    logger.debug(f"dhf: {drop_hidden_fields}")
    timings = {}
    TimeItContext = functools.partial(
        __TimeItContext__, report_dict=timings, print_callback=logger.debug
    )

    with TimeItContext("prep & tags"):
        task_list = ctx.obj["task_list"]
        _process_tag = TagProcessor(task_list.get_coll("tags"))
        tags, exclude_tags = list(map(_process_tag, tags)), list(
            map(_process_tag, exclude_tags)
        )

    with TimeItContext("fetch"):
        df = task_list.get_all_tasks(
            is_post_processing=out_format not in ["html"],
            is_drop_hidden_fields=(out_format not in ["html"])
            if drop_hidden_fields is None
            else drop_hidden_fields,
            tags=tags,
            exclude_tags=exclude_tags,
        )
        logger.debug(f"fetched {len(df)}")
    with TimeItContext("weekend"):
        before_date, after_date = map(parse_cmdline_datetime, [before_date, after_date])
        logger.debug((before_date, after_date))
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

    with TimeItContext("filter (status, tags)"):
        # df = df.query("status!='DONE' and status!='FAILED'")
        df = df[~df["status"].isin(json.loads(filter_out_states))]
        if len(tags) > 0:
            df = df[[set(_tags) >= set(tags) for _tags in df.tags]]
        if len(labels) > 0:
            for k, v in labels:
                df = df[
                    df["label"]
                    .apply(lambda x: {} if pd.isna(x) else x)
                    .apply(operator.methodcaller("get", k))
                    == v
                ]

    with TimeItContext("filter (DatesQueryEvaluator)"):
        if scheduled_date_query is not None:
            df = df[
                df["scheduled_date"].apply(DatesQueryEvaluator(scheduled_date_query))
            ]
    with TimeItContext("filter (dates misc)"):
        if un_scheduled and len(df) > 0:
            df = df[[pd.isna(sd) for sd in df["scheduled_date"]]]
        if len(when) > 0 and len(df) > 0:
            df = df[df["when"].isin(when)]
        if text is not None and len(df) > 0:
            df = df[df["name"].str.lower().apply(lambda s: text.lower() in s)]
        if before_date is not None and len(df) > 0:
            logger.debug((before_date, after_date))
            df = df[pd.to_datetime(df["scheduled_date"]).le(before_date)]
        if after_date is not None and len(df) > 0:
            df = df[pd.to_datetime(df["scheduled_date"]).ge(after_date)]

    with TimeItContext("filter (tags)"):
        ## FIXME takes long time (26s)
        df["tags"] = df["tags"].apply(
            lambda tags: sorted(map(_process_tag.tag_uuid_to_tag_name, tags))
        )

    with TimeItContext("filter (rels)"):
        # FIXME: this is slow (10 sec)
        ## example: http://127.0.0.1:5000/ls?profile=ttask&bd=today&ad=today&rels=c5312c12-63f8-4ced-944d-2329cd272496,Contains,outward
        logger.debug(f"r: {relations}")
        if relations is not None:
            (
                rel_task_uuid,
                rel_name,
                rel_dir,
                # relations_config,
                _,
            ) = relations
            df_relations = real_list_relations(
                ctx, rel_task_uuid, rel_name, direction_filter=rel_dir
            )
            logger.debug(
                (
                    "df_relations",
                    len(df_relations),
                    df_relations.to_dict(orient="index"),
                )
            )
            if len(df_relations) == 0:
                l = []
            else:
                l = df_relations[
                    "inward" if rel_dir == "outward" else "outward"
                ].to_list()
            logger.debug(f"l: {l}")
            df = df[df["uuid"].isin(l)]

    with TimeItContext("cut & sort"):
        if head is not None:
            df = df.head(head)
        if sample is not None:
            click_echo(f"{len(df)} tasks initially")
            df = df.sample(n=sample)

        if len(df) > 0:
            if sort_order:
                kwargs = cmdline_keys_to_sort_kwargs(sort_order)
                logger.debug(f"sort {kwargs}")
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

    with TimeItContext("pretty_df"):
        pretty_df = df.copy()
        pretty_df["tags"] = pretty_df["tags"].apply(", ".join)
        pretty_df["tags"] = pretty_df["tags"].apply(lambda s: f'"{s}"')

        if name_length_limit > 0:
            pretty_df.name = pretty_df.name.apply(
                lambda s: s
                if len(s) < name_length_limit
                else f"{s[:name_length_limit]}..."
            )

        if (len(columns) > 0) or (len(smart_columns) > 0):
            columns = list(columns)
            # pretty_df = pretty_df[columns]
            pretty_df = pd.DataFrame(
                {
                    **{cn: pretty_df[cn] for cn in columns},
                    **{
                        cn: smart_processor(pretty_df, proc)
                        for cn, proc in smart_columns
                    },
                }
            )

    with TimeItContext("format_df"):
        logger.debug((len(pretty_df), out_format))

        click_echo(
            format_df(
                pretty_df,
                "plain" if not out_format else out_format,
                formatters=dict(
                    html=lambda _: format_html(
                        df,
                        out_format_config,
                        task_list,
                        print_callback=click_echo,
                        out_file=out_file,
                    )
                ),
            )
        )

        s = f"{len(pretty_df)} tasks matched"
        if out_format in ["html"]:
            logger.debug(s)
        if out_format not in ["json", "html", "csv", "csvfn"]:
            click_echo(s)

    timings_df = pd.Series(timings).to_frame("duration_seconds")
    timings_df["dur"] = timings_df["duration_seconds"].apply(
        lambda s: timedelta(seconds=s)
    )
    timings_df["perc"] = timings_df["dur"] / timings_df["dur"].sum() * 100
    logger.debug(timings_df)


@gstasks.command()
@moption("-u", "--uuid-text", type=str, required=True)
@build_click_options
@click.pass_context
def actions_ls(ctx, uuid_text, **format_df_kwargs):
    df = real_actions_ls(ctx, uuid_text)
    click.echo(apply_click_options(df, format_df_kwargs))


def real_actions_ls(ctx, uuid_text) -> pd.DataFrame:
    task_list = ctx.obj["task_list"]
    r, _ = task_list.get_task(uuid_text=uuid_text)
    coll = task_list.get_coll("actions")
    df = pd.DataFrame(coll.find({"r.uuid": r["uuid"]}))
    return df


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
    """
    TODO:
    1. show planned
    """
    task_list = ctx.obj["task_list"]
    tasks_df = task_list.get_all_tasks(is_drop_hidden_fields=False)

    actions_df = pd.DataFrame(
        task_list.get_coll("actions").find(
            {"action": "replacing", "r.status": target_status}
        )
    )
    actions_df["uuid"] = actions_df["r"].apply(operator.itemgetter("uuid"))
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


@gstasks.group(name="rl")
@moption("-u", "--uuid-text", type=str, required=False)
@click.pass_context
def rolling_log(ctx, uuid_text):
    task_list = ctx.obj["task_list"]

    if uuid_text is None:
        uuid_text = get_last_engaged_task_uuid(task_list)
        assert uuid_text is not None
    else:
        r, _ = task_list.get_task(uuid_text=uuid_text)
        uuid_text = r["uuid"]

    ctx.obj["coll"] = task_list.get_coll("rolling_log")
    ctx.obj["uuid"] = uuid_text


@rolling_log.command(name="add")
@moption("-u", "--url", type=str, required=True)
@moption("-c", "--comment", type=str)
@moption("--omit-url-check/--no-omit-url-check", default=False)
@moption(
    "-d",
    "--date-time",
    type=SimpleCliDatetimeParamType(
        formats=[
            "%Y-%m-%d %H:%M",
            "%H:%M",
        ],
        short_dt_types={
            "%H:%M": {"year", "month", "day"},
        },
    ),
)
@click.pass_context
def rolling_log_add(ctx, url, comment, date_time, omit_url_check):
    real_rolling_log_add(
        task_uuid=ctx.obj["uuid"],
        coll=ctx.obj["coll"],
        url=url,
        comment=comment,
        omit_url_check=omit_url_check,
        date_time=date_time,
    )


@rolling_log.command(name="rm")
@moption("-u", "--uuid", "uuids", multiple=True)
@moption("-f", "--uuid-file", type=click.Path(allow_dash=True))
@click.pass_context
def rolling_log_rm(ctx, uuids, uuid_file):
    uuids = set(uuids)
    if uuid_file is not None:
        with click.open_file(uuid_file) as f:
            uuids.update(
                filter(
                    lambda s: len(s) > 0,
                    map(operator.methodcaller("strip"), f.readlines()),
                )
            )
    assert len(uuids) > 0
    logging.warning(uuids)
    for uuid_ in tqdm.tqdm(sorted(uuids)):
        res = ctx.obj["coll"].delete_one({"uuid": uuid_})
        logging.warning(f"rm: {res}")


@rolling_log.command(name="ls")
@moption("--raw/--no-raw", "-r/ ", default=False)
@build_click_options
@click.pass_context
def rolling_log_ls(ctx, raw, **format_df_kwargs):
    df = get_rolling_log_df(ctx, obj["uuid"])

    if raw:
        click.echo(apply_click_options(df, format_df_kwargs))
    else:
        click.echo(rolling_log_df_to_md_string(df))


def get_rolling_log_df(ctx, task_uuid: str) -> pd.DataFrame:
    df = pd.DataFrame(
        ctx.obj.get("coll", ctx.obj["task_list"].get_coll("rolling_log")).find(
            {"task_uuid": task_uuid}
        )
    )
    if len(df) == 0:
        logging.warning("rolling log is empty")
        return df
    df.sort_values(by="date_time", inplace=True, ignore_index=True)
    return df


def rolling_log_df_to_md_string(df: pd.DataFrame) -> str:
    df = df.copy()
    df["dt"] = df["date_time"].apply(operator.methodcaller("strftime", "%Y-%m-%d (%a)"))
    is_first = True
    res = ""
    for dt, slice_ in df.groupby("dt"):
        if not is_first:
            res += "\n"
        res += f"## `{dt}`\n"
        for i, r in enumerate(slice_.to_dict(orient="records")):
            res += Template(
                "1. [{{r.url}}]({{r.url}}){%if r.comment%} ({{r.comment}}){%endif%}\n"
            ).render(dict(r=r))
        is_first = False
    return res


@gstasks.group(name="rel")
@moption(
    "--relations-config-file",
    type=click.Path(),
    default=CLICK_DEFAULT_VALUES["relations"]["relations_config_file"],
)
@click.pass_context
def relations(ctx, relations_config_file):
    with open(relations_config_file) as f:
        config = json5.load(f)
    logging.warning(f"config: {config}")
    ctx.obj["relations"] = dict(config=config)


@relations.command(name="dot")
@click.pass_context
@moption("-f", "--uuid-file", type=click.Path(allow_dash=True), required=True)
@moption("-o", "--output-file", type=click.Path())
def dot_relations(ctx, uuid_file, output_file):
    """
    cat ~/Downloads/tag-tasks.uuid.txt|./gstasks.py rel dot -f-|dot -Tsvg  > /tmp/output.svg
    """
    task_list = ctx.obj["task_list"]
    with click.open_file(uuid_file) as f:
        uuids = sorted(set(f.read().strip().split()))
    logging.warning(uuids)

    coll = ctx.obj["task_list"].get_coll("relations")
    df_rel = pd.DataFrame(coll.find())
    df_rel = df_rel[df_rel["inward"].isin(uuids) | df_rel["outward"].isin(uuids)]
    logging.warning(df_rel)

    df_uuids = pd.DataFrame(
        [
            task_list.get_task(
                uuid_text=u,
                index=None,
                get_all_tasks_kwargs=dict(is_drop_hidden_fields=False),
            )[0]
            for u in sorted({*uuids, *df_rel["outward"], *df_rel["inward"]})
        ],
    )
    df_uuids.set_index("uuid", inplace=True)
    df_uuids = df_uuids[["name"]]
    df_uuids["is_core"] = df_uuids.index.to_series().isin(uuids)
    logging.warning(df_uuids)

    dot = graphviz.Digraph()
    for uuid in df_uuids.index:
        dot.node(uuid, df_uuids.loc[uuid, "name"])
    graphviz_available_arrow_shapes = [
        ## https://graphviz.org/doc/info/arrows.html
        "normal",
        "vee",
        "diamond",
    ]
    arrow_shapes = {}
    for i, o, name in df_rel[["inward", "outward", "relation_name"]].values:
        if name not in arrow_shapes:
            arrow_shapes[name] = graphviz_available_arrow_shapes.pop(0)
        dot.edge(i, o, label=name, arrowhead=arrow_shapes[name])

    if output_file is not None:
        dot.render(output_file)
    else:
        click.echo(dot.source)


@relations.command(name="ls")
@build_click_options
@moption("-u", "--uuid-text", type=GSTASK_UUID)
@moption("-d", "--direction-filter", type=click.Choice(["inward", "outward"]))
@moption("-n", "--relation-name", type=str)
@click.pass_context
def list_relations(ctx, uuid_text, relation_name, direction_filter, **format_df_kwargs):
    """"""
    df = real_list_relations(ctx, uuid_text, relation_name, direction_filter)
    click.echo(apply_click_options(df, format_df_kwargs))


def real_list_relations(
    ctx, uuid_text, relation_name: typing.Optional[str] = None, direction_filter=None
) -> pd.DataFrame:
    coll = ctx.obj["task_list"].get_coll("relations")

    filter_ = {}
    if relation_name is not None:
        filter_["relation_name"] = relation_name

    if uuid_text is None:
        df = pd.DataFrame(coll.find(filter_))
    else:
        if direction_filter is None:
            filter_["$or"] = [dict(inward=uuid_text), dict(outward=uuid_text)]
        elif direction_filter == "inward":
            filter_["inward"] = uuid_text
        elif direction_filter == "outward":
            filter_["outward"] = uuid_text
        else:
            raise NotImplementedError(dict(direction_filter=direction_filter))
        df = pd.DataFrame(coll.find(filter_))
        if len(df) == 0:
            return df
        for k in ["inward", "outward"]:
            for kk in ["name", "status"]:
                df[f"{k}_{kk}"] = (
                    df[k]
                    .apply(lambda u: ctx.obj["task_list"].get_task(uuid_text=u))
                    .apply(operator.itemgetter(0))
                    .apply(operator.itemgetter(kk))
                )
        df.drop(columns=["_id"], inplace=True)
    return df


# @relations.command(name="import")
# @moption("-f", "--file-name", type=click.Path(allow_dash=True))
# @moption("--pre-clean/--no-pre-clean", "-p/ ", default=False)
# @click.pass_context
# def import_relations(ctx, file_name, pre_clean):
#     raise NotImplementedError()  # 1


@relations.command(name="add")
@moption("-f", "--from", "froms", type=GSTASK_UUID, required=True, multiple=True)
@moption("-t", "--to", "tos", type=GSTASK_UUID, required=True, multiple=True)
@moption("-n", "--relation-name", type=str, required=True)
@click.pass_context
def add_relation(ctx, froms, tos, relation_name):
    rel_config = ctx.obj["relations"]["config"]
    assert relation_name in rel_config, (relation_name, rel_config)

    coll = ctx.obj["task_list"].get_coll("relations")
    for from_, to in itertools.product(froms, tos):
        r = dict(
            inward=to,
            outward=from_,
            uuid=str(uuid.uuid4()),
            relation_name=relation_name,
        )
        logging.warning(f"rel: {r}")

        coll.insert_one(
            filter={k: v for k, v in r.items() if k != "uuid"},
            new_values={"uuid": r["uuid"]},
            usert=True,
        )


@relations.command(name="rm")
@moption("-u", "--uuud", "uuid_", type=str, required=True)
@click.pass_context
def delete_relation(ctx, uuid_):
    coll = ctx.obj["task_list"].get_coll("relations")
    res = coll.delete_one(dict(uuid=uuid_))
    logging.warning(f"del_res: {res}")


@gstasks.group()
@moption(
    "--jira-config-json5",
    type=click.Path(),
    required=True,
    default=".jira-config.json5",
)
@click.pass_context
def jira(ctx, jira_config_json5):
    # assert jira_label == DEFAULT_JIRA_LABEL, (jira_label, DEFAULT_JIRA_LABEL, "FIXME")
    ctx.obj["jira"] = dict()
    with open(jira_config_json5) as f:
        jh = JiraHelper(**json5.load(f))
    logging.warning(f"jh: {jh}")
    ctx.obj["jira"]["helper"] = jh


@jira.command(name="link")
@moption("-u", "--gstask-uuid")
@moption("-i", "--jira-id")
@click.pass_context
def jira_link(ctx, gstask_uuid, jira_id):
    task_list = ctx.obj["task_list"]
    jh = ctx.obj["jira"]["helper"]

    g, j = gstask_uuid is not None, jira_id is not None
    if (g, j) == (False, False):
        raise NotImplementedError(
            "at least one of `jira_id` or `gstask_uuid` has to be non-null!"
        )
    elif (g, j) == (False, True):
        raise NotImplementedError("TODO")
    elif (g, j) == (True, False):
        r, idx = task_list.get_task(
            uuid_text=gstask_uuid,
            index=None,
        )
        out = jh.run_operation(
            "add_issue",
            name=r["name"],
            description=gstask_uuid_to_sigil(r["uuid"], prefix="master:"),
        )
        out = json.loads(out)
        # https://nailbiter91.atlassian.net/browse/ML3-98
        key = out["key"]
        domain_name = out["self"].split("/")[2]
        url = f"https://{domain_name}/browse/{key}"
        label = r.get("label", {})
        r["label"] = {**label, jh.jira_label: url}
        logging.warning(r)
        task_list.insert_or_replace_record(r, index=idx)
    elif (g, j) == (True, True):
        r, idx = task_list.get_task(
            uuid_text=gstask_uuid,
            index=None,
        )
        # check jira ticket has no sigil
        # edit description
        # link
        raise NotImplementedError("TODO")


def gstask_uuid_to_sigil(gstask_uuid: str, prefix: typing.Optional[str] = None) -> str:
    res = f"gstask:{gstask_uuid}"
    return res if prefix is None else prefix + res


@jira.command(name="import")
@moption("-l", "--from-to", type=(str, str), multiple=True)
@moption("-i", "--id", "ids", multiple=True)
@moption("-s", "--scheduled-date", required=True, type=CLI_DATETIME)
@click.pass_context
def import_jira(ctx, from_to, ids, scheduled_date):
    ids = [
        *ids,
        *itertools.chain.from_iterable(
            map(lambda t: generate_symbols_between(*t), from_to)
        ),
    ]
    assert len(ids) > 0, (from_to, ids)
    logging.warning(ids)
    jh = ctx.obj["jira"]["helper"]
    names = jh.get("name", ids)
    logging.warning(names)


@gstasks.command()
@moption(
    "-f",
    "--uuid-list-file",
    default="-",
    # required=True,
    type=click.Path(allow_dash=True),
    help="json array {uuid,name}",
)
@moption("-g", "--filter-tag", "filter_tags", type=str, multiple=True)
@click.pass_context
def auto_tag(ctx, uuid_list_file, filter_tags):
    """
    examples:
    $ ./gstasks.py ls -o plain -c name -c uuid -o json|./gstasks.py auto-tag -f- -g borg|jq '.[]|.uuid' -r|./gstasks.py edit -f- -g borg --create-new-tag
    """
    with click.open_file(uuid_list_file) as f:
        tasks_df = pd.DataFrame(json.load(f))
    tasks_df["tags"] = (
        tasks_df["name"].apply(re.compile(r"#([a-z_0-9]+)").findall).apply(set)
    )

    tasks_df = tasks_df[tasks_df["tags"].apply(lambda s: s >= set(filter_tags))]

    logging.warning(tasks_df)
    tasks_df["tags"] = tasks_df["tags"].apply(sorted)
    click.echo(json.dumps(tasks_df.to_dict(orient="records")))


@gstasks.group()
@click.option(
    "--habits-file",
    type=click.Path(),
    default=path.join(path.dirname(__file__), ".habits.json5"),
)
@click.pass_context
def habits(ctx, habits_file):
    if path.isfile(habits_file):
        with open(habits_file) as f:
            habits = json5.load(f)
    else:
        habits = {}

    habits["config"] = habits.get("config", {})
    habits["config"]["backfill_collection_name"] = habits["config"].get(
        "backfill_collection_name", "habits_backfill"
    )

    ctx.obj["backfill_coll"] = ctx.obj["task_list"].get_coll(
        habits["config"]["backfill_collection_name"]
    )

    ctx.obj["habits"] = habits["habits"]


@habits.command()
@click.option("--dry-run/--no-dry-run", default=False)
@click.option("--run-command/--no-run-command", default=True)
@click.option("-F", "--habits-filter-regex")
@click.pass_context
def backfill(ctx, dry_run, run_command, habits_filter_regex):
    backfill_coll = ctx.obj["backfill_coll"]
    habits_df = pd.DataFrame(ctx.obj["habits"].values(), index=ctx.obj["habits"].keys())
    if habits_filter_regex is not None:
        habits_df = habits_df[
            habits_df.index.to_series()
            .apply(re.compile(habits_filter_regex).search)
            .notna()
        ]
    logging.warning(habits_df)

    backfill_df = pd.DataFrame(backfill_coll.find(), columns=["name", "dt", "is_done"])
    logging.warning(backfill_df)

    punches_df = pd.concat(
        {
            habit_name: pd.DataFrame(
                dict(
                    dt=generate_habits_series(
                        start=datetime.strptime(habit_d["start"], "%Y-%m-%d"),
                        cronline=habit_d["cronline"],
                    ),
                    command_json=json.dumps(habit_d["command"]),
                )
            ).set_index("dt")
            for habit_name, habit_d in tqdm.tqdm(
                habits_df.to_dict(orient="index").items()
            )
        },
        names=["name", "dt"],
    )
    punches_df.reset_index(inplace=True)

    punches_df = punches_df.merge(
        backfill_df, left_on=["name", "dt"], right_on=["name", "dt"], how="left"
    )
    punches_df["is_done"] = punches_df["is_done"].fillna(False)
    logging.warning(punches_df)

    punches_df = punches_df[~punches_df["is_done"]]
    logging.warning(f"{len(punches_df)} items to backfill")

    dr = "x" if dry_run else "o"
    for r in tqdm.tqdm(punches_df.to_dict(orient="records")):
        command = Template(r["command_json"]).render(dict(r))
        logging.warning(f"{dr}> {command}")
        if not dry_run:
            cmd, *args, kwargs = json.loads(command)
            if run_command:
                dict(add=functools.partial(real_add, ctx))[cmd](*args, **kwargs)
            r["is_done"] = True
            backfill_coll.replace_one(
                filter={k: r[k] for k in ["dt", "name"]},
                replacement={k: r[k] for k in ["dt", "name", "is_done"]},
                upsert=True,
            )


@gstasks.group()
@moption("-u", "--uuid", "uuid_", type=GSTASK_UUID, required=True)
@click.pass_context
def worktime(ctx, uuid_):
    ctx.obj["worktime_coll"] = ctx.obj["task_list"].get_coll("worktime")
    ctx.obj["worktime_uuid"] = uuid_


@worktime.command(name="rm")
@moption("-u", "--uuid", "uuid_", type=str, required=True)
@click.pass_context
def worktime_rm(ctx, uuid_):
    logging.warning(ctx.obj["worktime_coll"].delete_one({"uuid": uuid_}))


@worktime.command(name="ls")
@build_click_options
@click.pass_context
def worktime_ls(ctx, **format_df_kwargs):
    logging.warning({k: v for k, v in ctx.obj.items() if k.startswith("worktime_")})
    df = real_worktime_ls(
        coll=ctx.obj["worktime_coll"], task_uuid=ctx.obj["worktime_uuid"]
    )
    click.echo(apply_click_options(df, format_df_kwargs))


@worktime.command(name="add")
@moption("-m", "--duration-min", type=int, required=True)
@moption("-n", "--now", type=click.DateTime())
@moption("-c", "--comment", type=str)
@click.pass_context
def worktime_add(ctx, duration_min, now, comment):
    logging.warning({k: v for k, v in ctx.obj.items() if k.startswith("worktime_")})
    click.echo(
        real_worktime_add(
            coll=ctx.obj["worktime_coll"],
            task_uuid=ctx.obj["worktime_uuid"],
            now=now,
            duration_sec=60 * duration_min,
            comment=comment,
        )
    )


if __name__ == "__main__":
    env_fnss = [
        [path.join(path.dirname(path.abspath(__file__)), ".gstasks.env"), ".env"],
        [path.join(path.dirname(path.abspath(__file__)), ".gstasks.checked.env")],
    ]
    for env_fns in env_fnss:
        for env_fn in env_fns:
            if path.isfile(env_fn):
                LOADED_DOTENVS.append(env_fn)
                load_dotenv(dotenv_path=env_fn, override=True)
                break

    gstasks(show_default=True, auto_envvar_prefix="GSTASKS")
