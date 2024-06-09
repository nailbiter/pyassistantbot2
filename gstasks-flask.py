#!/usr/bin/env python3
"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/gstasks-flask.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-05-03T22:14:01.034153
    REVISION: ---

==============================================================================="""
from flask import Flask, request, g, render_template
from dotenv import load_dotenv
import os
from os import path
import subprocess
import uuid
import logging
import json
from jinja2 import Template
import tqdm
from _gstasks.timing import TimeItContext
from _gstasks import TaskList, str_or_envvar
import pandas as pd
from datetime import datetime, timedelta
import collections
from gstasks import real_ls, real_lso, real_edit
import pymongo
import json5
import typing

MockClickContext = collections.namedtuple("MockClickContext", "obj", defaults=[{}])
## https://stackoverflow.com/a/42791810
app = Flask(__name__, static_url_path="", static_folder="gstasks-flask-static")
# my_g = {}


def _get_habits(mongo_url: str) -> pd.DataFrame:
    logging.warning(f"mongo_url: {mongo_url}")
    mongo_client = pymongo.MongoClient(mongo_url)
    _now = datetime.now()
    filter_ = {
        "$and": [
            {"due": {"$gt": _now - timedelta(hours=9)}},
            {"status": {"$exists": False}},
        ]
    }
    coll = mongo_client["logistics"]["alex.habitspunch2"]
    df = pd.DataFrame(coll.find(filter=filter_))
    df[["due", "date"]] += timedelta(hours=9)
    df.drop(columns=["_id"], inplace=True)
    df.sort_values(by=["due", "name"], ascending=[False, True], inplace=True)

    return df


# @app.before_first_request
# def before_first_request():
#     g.test = 1
def _init_g(g, mongo_url: typing.Optional[str]):
    if hasattr(g, "ctx"):
        logging.warning(f"`g` has `ctx` (={g.ctx}) ==> do nothing")
    else:
        logging.warning(f"`g` has no `ctx` ==> init")
        g.ctx = MockClickContext()
        g.ctx.obj["task_list"] = TaskList(
            mongo_url=mongo_url,
            database_name="gstasks",
            collection_name="tasks",
        )


@app.route("/lso/<uuid:task_id>", methods=["GET"])
def lso(task_id: str):
    logging.warning(f"task_id: {task_id}")
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    # return str(task_id)
    res = real_lso(g.ctx, [str(task_id)], object_type="task", is_loud=False)
    logging.warning(f"res: {res}")
    # return res
    # return f"<code>{json.dumps(json.loads(res),sort_keys=True,indent=2)}</code>"
    # return pd.Series(json.loads(res)).to_frame().sort_index().to_html()
    # return json.dumps(json.loads(res), sort_keys=True, indent=2)
    s_html = pd.Series(json.loads(res)).to_frame().sort_index().to_html()
    return render_template("lso.jinja.html", s_html=s_html, res=json.loads(res))


@app.route("/edit/<uuid:task_id>", methods=["POST"])
def edit(task_id):
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)

    logging.warning(f"ti: {task_id}")
    logging.warning(f"form: {request.form}")
    form = dict(request.form)
    logging.warning(f"form: {form}")

    kwargs = dict(status=form["status"], scheduled_date=None, due=None, tags=[])

    real_edit(
        ctx=g.ctx,
        uuid_text=[str(task_id)],
        **kwargs,
    )

    return "hi"


def _init() -> (dict, str):
    gstasks_settings_fn = os.environ.get(
        "GSTASKS_FLASK_GSTASKS_SETTINGS", ".gstasks_flask_settings.json5"
    )
    with open(gstasks_settings_fn) as f:
        gstasks_settings = json5.load(f)

    mongo_url = (
        str_or_envvar(gstasks_settings["mongo_url"])
        if "mongo_url" in gstasks_settings
        else None
    )
    return gstasks_settings, mongo_url


@app.route("/ls", methods=["GET"])
def ls():
    # logging.warning(f"g: {my_g}")
    timings = {}

    with TimeItContext("init", report_dict=timings):
        gstasks_settings, mongo_url = _init()
        _init_g(g, mongo_url=mongo_url)
        gstasks_profiles = gstasks_settings["profiles"]
        gstasks_exe = gstasks_settings["gstasks_exe"]

    with TimeItContext("parse flask", report_dict=timings):
        args = request.args
        args = request.args.to_dict()

        if "profile" in args:
            profile = args.pop("profile")
        else:
            profile = "standard"
        if "tag" in args:
            tag = args.pop("tag")
        else:
            tag = None
        ## http://127.0.0.1:5000/ls?profile=ttask&bd=tomorrow&ad=tomorrow
        if "ad" in args:
            after_date = args.pop("ad")
        else:
            after_date = None
        if "bd" in args:
            before_date = args.pop("bd")
        else:
            before_date = None

        logging.warning(
            dict(
                args=args,
                profile=profile,
                tag=tag,
                after_date=after_date,
                before_date=before_date,
            )
        )

    with TimeItContext("widgets", report_dict=timings):
        gstasks_profile = gstasks_profiles[profile]
        jinja_env = {"widgets": {}}
        for widget, widget_config in gstasks_profile.get("widgets", {}).items():
            if widget == "habits":
                jinja_env["widgets"]["habits_df"] = _get_habits(
                    str_or_envvar(widget_config["mongo_url"])
                    if "mongo_url" in widget_config
                    else None
                )
            elif widget == "tags":
                tags_df = pd.DataFrame(dict(tag_name=["tag"], cnt=[999]))
                mongo_client = pymongo.MongoClient(
                    str_or_envvar(widget_config["mongo_url"])
                    if "mongo_url" in widget_config
                    else None
                )
                tags_df = (
                    pd.DataFrame(mongo_client["gstasks"]["tags"].find())
                    .drop(columns=["_id"])
                    .merge(
                        pd.DataFrame(
                            mongo_client["gstasks"]["tasks"].aggregate(
                                [
                                    {
                                        "$match": {
                                            "status": widget_config.get("match_status")
                                        }
                                    },
                                    {"$unwind": "$tags"},
                                    {
                                        "$group": {
                                            "_id": "$tags",
                                            "count": {"$sum": 1},
                                            "due": {"$min": "$due"},
                                        }
                                    },
                                ]
                            )
                        ),
                        how="inner",
                        left_on="uuid",
                        right_on="_id",
                    )[["name", "count", "due"]]
                )
                tags_df.rename(columns={"name": "tag_name"}, inplace=True)
                tags_df.set_index("tag_name", inplace=True)
                tag_names = widget_config.get("tags", [])
                if len(tag_names) > 0:
                    tags_df = tags_df.loc[[x for x in tag_names if x in tags_df.index]]
                else:
                    tags_df.sort_index(inplace=True)
                tpl = Template(
                    widget_config.get(
                        "tag_url_tpl",
                        """<a href="ls?profile={{profile}}&tag={{name}}">{{name}}</a>""",
                    )
                )
                tags_df.index = (
                    tags_df.index.to_series()
                    .apply(lambda name: tpl.render(dict(name=name, profile=profile)))
                    .to_list()
                )
                jinja_env["widgets"]["tags_df"] = tags_df
            else:
                logging.error(dict(widget=widget))

    with TimeItContext("run", report_dict=timings):
        out_fns = {}
        keys = " ".join([f"-{k} {v}" for k, v in args.items()])
        for k, v in tqdm.tqdm(gstasks_profile["blocks"].items()):
            out_fn = f"/tmp/{uuid.uuid4()}.html"
            if gstasks_profile.get("is_use_shell", False):
                cmd = Template(v["cmd"]).render(
                    dict(
                        gstasks_exe=gstasks_exe,
                        keys=keys,
                        out_fn=out_fn,
                        tag=tag,
                        after_date=after_date,
                        before_date=before_date,
                    )
                )
                logging.warning(f"cmd: `{cmd}`")
                ec, out = subprocess.getstatusoutput(cmd)
                assert ec == 0, (cmd, ec, out)
            else:
                kwargs = {
                    **v["kwargs"],
                    "out_file": out_fn,
                    "ctx": g.ctx,
                }
                if tag is not None:
                    kwargs["tags"] = [tag]
                for kk, vv in dict(
                    before_date=before_date, after_date=after_date
                ).items():
                    if vv is not None:
                        kwargs[kk] = vv
                logging.warning(kwargs)
                real_ls(**kwargs)
            out_fns[k] = out_fn

    with TimeItContext("read output", report_dict=timings):
        outs = {}
        for k, out_fn in out_fns.items():
            logging.warning(out_fn)
            with open(out_fn) as f:
                out = f.read()
            outs[k] = out
        if "template" in gstasks_profile:
            with open(gstasks_profile["template"]) as f:
                template = f.read()
            jinja_env["table_htmls"] = outs
            jinja_env["static_files"] = {}
            for k, fn in gstasks_profile.get("static_files", {}).items():
                with open(fn) as f:
                    jinja_env["static_files"][k] = f.read()
            out = Template(template).render({**jinja_env})

    timings_df = pd.Series(timings).to_frame("duration_seconds")
    timings_df["dur"] = timings_df["duration_seconds"].apply(
        lambda s: timedelta(seconds=s)
    )
    timings_df["perc"] = timings_df["dur"] / timings_df["dur"].sum() * 100
    logging.warning(timings_df)

    return out


if __name__ == "__main__":
    env_fns = [
        path.join(path.dirname(path.abspath(__file__)), ".gstasks.env"),
        ".env",
    ]
    for env_fn in env_fns:
        if path.isfile(env_fn):
            # LOADED_DOTENV = env_fn
            load_dotenv(dotenv_path=env_fn)
            break

    port = int(os.environ.get("PORT", 5000))
    # my_g["test"] = 1
    app.run(host="0.0.0.0", port=port)
