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
from jinja2 import Template
from _common import parse_cmdline_datetime, run_trello_cmd, get_random_fn
from _gstasks import CLI_DATETIME, TagProcessor, TaskList, UuidCacher
from _gstasks.additional_states import ADDITIONAL_STATES
from _gstasks.html_formatter import format_html, ifnull
import requests

# FIXME: do without global env
LOADED_DOTENV = None

# If modifying these scopes, delete the file token.google_spreadsheet.pickle.
_SCOPES = [
    #    'https://www.googleapis.com/auth/spreadsheets.readonly',
    "https://www.googleapis.com/auth/spreadsheets",
]

option_with_envvar_explicit = functools.partial(click.option, show_envvar=True)


# @click.group(chain=True) #cannot do, because have subcommands
@click.group()
@option_with_envvar_explicit("--list-id", required=True)
@option_with_envvar_explicit("--mongo-url", required=True)
@option_with_envvar_explicit(
    "--uuid-cache-db",
    default=path.abspath(path.join(path.dirname(__file__), ".uuid_cache.db")),
)
@option_with_envvar_explicit("-d", "--debug")
@click.pass_context
def gstasks(ctx, list_id, mongo_url, uuid_cache_db, debug):
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
    ctx.obj["list_id"] = list_id
    ctx.obj["uuid_cache_db"] = uuid_cache_db


@gstasks.command()
@option_with_envvar_explicit("-t", "--tag", "tags")
@option_with_envvar_explicit("--contains")
@option_with_envvar_explicit("--not-contains")
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
@option_with_envvar_explicit("-h", "--task-hash")
@option_with_envvar_explicit(
    "-w",
    "--when",
    type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")),
)
@option_with_envvar_explicit("-s", "--scheduled-date")
@option_with_envvar_explicit("--archive/--no-archive", default=True)
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
@option_with_envvar_explicit("-i", "--index", type=int, multiple=True)
@option_with_envvar_explicit("-u", "--uuid-text", multiple=True)
@option_with_envvar_explicit(
    "--web-browser",
)
@option_with_envvar_explicit("--open-url/--no-open-url", default=True)
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
@option_with_envvar_explicit("-u", "--uuid-text", multiple=True)
@option_with_envvar_explicit("-i", "--index", type=int, multiple=True)
@option_with_envvar_explicit("--create-archived/--no-create-archived", default=True)
@option_with_envvar_explicit(
    "-l",
    "--label",
    multiple=True,
)
@option_with_envvar_explicit(
    "--web-browser",
)
@option_with_envvar_explicit("--open-url/--no-open-url", default=False)
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
@option_with_envvar_explicit("-u", "--uuid-text", multiple=True)
@option_with_envvar_explicit("-i", "--index", type=int, multiple=True)
@option_with_envvar_explicit("-f", "--uuid-list-file", type=click.Path())
@option_with_envvar_explicit("-n", "--name")
@option_with_envvar_explicit(
    "-t",
    "--status",
    type=click.Choice(["DONE", "FAILED", "REGULAR", *ADDITIONAL_STATES]),
)
@option_with_envvar_explicit(
    "-w",
    "--when",
    type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")),
)
@option_with_envvar_explicit("-s", "--scheduled-date")
@option_with_envvar_explicit("-g", "--tag", "tags", multiple=True)
@option_with_envvar_explicit(
    "--tag-operation",
    type=click.Choice(["symmetric_difference", "union", "difference"]),
    default="symmetric_difference",
)
@option_with_envvar_explicit("--url", "URL")
# FIXME: allow `NONE` for `due` (use more carefully-written version of `parse_cmdline_datetime`)
# FIXME: allow `NONE` for everything else
@option_with_envvar_explicit("-d", "--due")
@option_with_envvar_explicit("-a", "--action-comment")
@option_with_envvar_explicit("-c", "--comment")
@option_with_envvar_explicit("--create-new-tag/--no-create-new-tag", default=False)
@option_with_envvar_explicit("-l", "--label", type=(str, str))
@option_with_envvar_explicit("--post-hook")
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
        with open(uuid_list_file) as f:
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
@option_with_envvar_explicit("-n", "--name", required=True)
@option_with_envvar_explicit(
    "-w",
    "--when",
    type=click.Choice("WEEKEND,EVENING,PARTTIME".split(",")),
    required=True,
)
@option_with_envvar_explicit("-u", "--url", "URL")
@option_with_envvar_explicit("-s", "--scheduled-date", type=CLI_DATETIME)
@option_with_envvar_explicit(
    "-t", "--status", type=click.Choice(["REGULAR", "DONE", *ADDITIONAL_STATES])
)
@option_with_envvar_explicit("-g", "--tag", "tags", multiple=True)
@option_with_envvar_explicit("-d", "--due", type=CLI_DATETIME)
@option_with_envvar_explicit("-c", "--comment")
@option_with_envvar_explicit("--create-new-tag/--no-create-new-tag", default=False)
@option_with_envvar_explicit("--post-hook")
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
@option_with_envvar_explicit("--remove-tag-from/--no-remove-tag-from", default=False)
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
@option_with_envvar_explicit(
    "-u", "--uuid-text", required=True, help='`D` means "disengage"'
)
@option_with_envvar_explicit("--post-hook")
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
    pass


@remind.command(name="add")
@option_with_envvar_explicit("-u", "--uuid-text", required=True)
@option_with_envvar_explicit("-d", "--remind-datetime", type=click.DateTime())
@click.pass_context
def add_remind(ctx, uuid_text, remind_datetime):
    if remind_datetime is None:
        remind_datetime = datetime.now()
    task_list = ctx.obj["task_list"]
    r, _ = task_list.get_task(uuid_text=uuid_text)
    coll = task_list.get_coll("remind")
    rem = dict(task_uuid=r["uuid"], remind_datetime=remind_datetime, sweeped_on=None)
    logging.warning(f"inserting rem: {rem}")
    coll.insert_one(rem)


@remind.command(name="ls")
@click.pass_context
def ls_remind(ctx):
    task_list = ctx.obj["task_list"]
    coll = task_list.get_coll("remind")
    df = pd.DataFrame(coll.find())
    click.echo(df.to_csv(sep="\t", index=None))


@remind.command(name="sweep")
@option_with_envvar_explicit("--dry-run/--no-dry-run", default=False)
@option_with_envvar_explicit("-s", "--slack-url")
@click.pass_context
def sweep_remind(ctx, dry_run, slack_url):
    task_list = ctx.obj["task_list"]
    coll = task_list.get_coll("remind")
    df = pd.DataFrame(coll.find({"sweeped_on": None}))
    now = datetime.now()
    if len(df)>0:
        df = df[df["remind_datetime"] <= now]
        logging.warning(df)
        if not dry_run:
            for _id in df["_id"]:
                # FIXME: use `update_many`
                coll.update_one({"_id": _id}, {"$set": {"sweeped_on": now}})
        if slack_url is not None and len(df) > 0:
            logging.warning(slack_url)
            requests.post(
                slack_url,
                json.dumps(
                    {
                        "text": Template(
                            "reminder on `{{now.isoformat()}}`\n```{{df.to_string()}}```"
                        ).render(dict(now=now, df=df.drop(columns=["_id", "sweeped_on"])))
                    }
                ),
                headers={"Content-type": "application/json"},
            )

            

@gstasks.command()
@option_with_envvar_explicit(
    "-w",
    "--when",
    multiple=True,
    type=click.Choice("WEEKEND,EVENING,PARTTIME,appropriate,all".split(",")),
)
@option_with_envvar_explicit("-x", "--text")
@option_with_envvar_explicit("-b", "--before-date")
@option_with_envvar_explicit("-a", "--after-date")
@option_with_envvar_explicit("-u", "--un-scheduled", is_flag=True, default=False)
@option_with_envvar_explicit(
    "-o", "--out-format", type=click.Choice(["str", "csv", "json", "html"])
)
@option_with_envvar_explicit("-h", "--head", type=int)
@option_with_envvar_explicit("-s", "--sample", type=int)
@option_with_envvar_explicit("--name-lenght-limit", type=int, default=50)
@option_with_envvar_explicit("-g", "--tag", "tags", multiple=True)
@option_with_envvar_explicit(
    "--out-format-config",
    type=click.Path(dir_okay=False, exists=True),
)
@option_with_envvar_explicit("-q", "--scheduled-date-query")
@option_with_envvar_explicit("--out-file", type=click.Path())
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
