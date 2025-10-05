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
from flask import Flask, request, g, render_template, send_file
from dotenv import load_dotenv
import os
from os import path
import subprocess
import markdown
import uuid
import logging
import json
from jinja2 import Template
import tqdm
from _gstasks.timing import TimeItContext
from _gstasks.additional_states import ADDITIONAL_STATES
from _gstasks import (
    TaskList,
    str_or_envvar,
    urllize_df,
    CLICK_DEFAULT_VALUES,
    real_worktime_add,
    real_worktime_ls,
    real_rolling_log_add,
)
import pandas as pd
from datetime import datetime, timedelta
import collections
from gstasks import (
    real_ls,
    real_list_relations,
    real_actions_ls,
    real_mark,
    real_lso,
    real_edit,
    _NONE_CLICK_VALUE,
    rolling_log_df_to_md_string,
    get_rolling_log_df,
    MARK_UNSET_SYMBOL,
)
import pymongo
import json5
import json
import base64
import typing
from bson import json_util
import functools
from _gstasks.flask.widgets import WidgetTags
from _gstasks.my_logging import get_configured_logger

robust_json_dumps = functools.partial(json.dumps, default=json_util.default)
logger = get_configured_logger(
    "gstasks-flask",
    level="DEBUG" if os.environ.get("GSTASKS_FLASK_DEBUG", "0") == "1" else "INFO",
)


# MockClickContext = collections.namedtuple("MockClickContext", "obj", defaults=[{}])
class MockClickContext:
    def __init__(self, mongo_url: str):
        self.obj = {}
        self.obj["task_list"] = TaskList(
            mongo_url=mongo_url,
            database_name="gstasks",
            collection_name="tasks",
        )


## https://stackoverflow.com/a/42791810
app = Flask(__name__, static_url_path="", static_folder="gstasks-flask-static")
# my_g = {}


def _get_habits(mongo_url: str) -> pd.DataFrame:
    logger.info(f"mongo_url: {mongo_url}")
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
        logger.info(f"`g` has `ctx` (={g.ctx}) ==> do nothing")
    else:
        logger.info(f"`g` has no `ctx` ==> init")
        g.ctx = MockClickContext(mongo_url)


_NOTHING_TEXT_FORM_VALUE = "**NOTHING**"
_NONE_TEXT_FORM_VALUE = "**NONE**"


@app.route("/run_script", methods=["GET"])
def run_script():
    gstasks_settings, _ = _init()
    scripts: dict = gstasks_settings.get("scripts", {})
    logging.info(f"scripts: {scripts}")

    args = request.args
    logger.info(f"args: {args}")
    args = request.args.to_dict()
    logger.info(f"args: {args}")

    script = scripts[args["name"]]
    ec, out = subprocess.getstatusoutput(script)
    return f"<pre>{out}</pre>"


@app.route("/list_scripts", methods=["GET"])
def list_scripts():
    gstasks_settings, _ = _init()
    scripts: dict = gstasks_settings.get("scripts", {})
    logging.info(f"scripts: {scripts}")

    return Template(
        """
    <ul>
    {% for k in scripts.keys() -%}
        <li><a href="/run_script?name={{k}}">{{k}}</a></li>
    {% endfor -%}
    </ul>
    """
    ).render(scripts=scripts)


@app.route("/lso/<uuid:task_id>", methods=["GET"])
def lso(task_id: str):
    logger.info(f"task_id: {task_id}")
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    # return str(task_id)
    res = real_lso(g.ctx, [str(task_id)], object_type="task", is_loud=False)
    logger.info(f"res: {res}")

    relations_config_file = os.environ.get(
        "GSTASKS_REL_RELATIONS_CONFIG_FILE",
        CLICK_DEFAULT_VALUES["relations"]["relations_config_file"],
    )
    logger.info(relations_config_file)
    with open(relations_config_file) as f:
        relations_config = json5.load(f)
    logger.info(relations_config)

    kwargs = dict(
        # s_html=s_html,
        res=json.loads(res),
        states=["DONE", "FAILED", *ADDITIONAL_STATES, _NOTHING_TEXT_FORM_VALUE],
        relations_config=relations_config,
        self_task_uuid=str(task_id),
        special_values=dict(
            nothing=_NOTHING_TEXT_FORM_VALUE, none=_NONE_TEXT_FORM_VALUE
        ),
        utils=dict(
            real_list_relations=functools.partial(
                _real_list_relations, uuid_text=str(task_id), ctx=g.ctx
            ),
            pd=pd,
        ),
    )

    jinja_template_fn = fn = os.environ.get("GSTASKS_FLASK_LSO_TEMPLATE")
    logger.info(f"jinja_template_fn: {jinja_template_fn}")
    if jinja_template_fn is None:
        return render_template("lso.jinja.html", **kwargs)
    else:
        with open(jinja_template_fn) as f:
            tpl = Template(f.read())
        return tpl.render(kwargs)


def _real_list_relations(
    ctx=None, uuid_text=None, is_urllize: bool = False
) -> pd.DataFrame:
    relations_df = real_list_relations(ctx, uuid_text=uuid_text)
    return (
        urllize_df(relations_df, cns=["inward", "outward"])
        if is_urllize
        else relations_df
    )


@app.route("/rolling_log/<uuid:task_id>", methods=["GET"])
def rolling_log(task_id: str) -> str:
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    task_id = str(task_id)
    rolling_log_df = get_rolling_log_df(g.ctx, task_id)
    if len(rolling_log_df) == 0:
        return "no rolling log"
    else:
        md_s = rolling_log_df_to_md_string(rolling_log_df)
        md = markdown.Markdown()
        logger.info(md)
        return md.convert(md_s)


@app.route("/relationships_list/<uuid:task_id>", methods=["GET"])
def relationships_list(task_id: str) -> str:
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    df = real_list_relations(g.ctx, uuid_text=str(task_id))
    return df.to_html()


@app.route("/activity_list/<uuid:task_id>", methods=["GET"])
def activity_list(task_id: str) -> str:
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    df = real_actions_ls(g.ctx, str(task_id))
    return df.to_html()


@app.route("/worktime_list/<uuid:task_id>", methods=["GET"])
def worktime_list(task_id: str) -> str:
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    df = real_worktime_ls(
        coll=g.ctx.obj["task_list"].get_coll("worktime"), task_uuid=str(task_id)
    )
    if "comment" in df.columns:
        df["comment"] = df["comment"].fillna("")
    df["duration"] = df.pop("duration_sec").apply(
        lambda seconds: timedelta(seconds=seconds)
    )
    return f"{df.to_html()}<br>total: {df['duration'].sum()}"


@app.route("/rolling_log_add/<uuid:task_id>", methods=["POST"])
def rolling_log_add(task_id) -> str:
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    task_id = str(task_id)

    logger.info(f"form: {request.form}")
    form = dict(request.form)
    assert form["url"] != "", form

    res = real_rolling_log_add(
        coll=g.ctx.obj["task_list"].get_coll("rolling_log"),
        task_uuid=str(task_id),
        url=form["url"],
        comment=None if form["comment"] == "" else form["comment"],
        date_time=None
        if form["date_time"] == ""
        else pd.to_datetime(form["date_time"]),
    )

    return f"<code>{res}</code>"


@app.route("/worktime_add/<uuid:task_id>", methods=["POST"])
def worktime_add(task_id) -> str:
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    task_id = str(task_id)

    logger.info(f"form: {request.form}")
    form = dict(request.form)
    duration_min = int(form["duration_min"])
    assert duration_min != 0

    res = real_worktime_add(
        coll=g.ctx.obj["task_list"].get_coll("worktime"),
        task_uuid=str(task_id),
        duration_sec=60 * duration_min,
        comment=form["comment"],
    )

    return f"<code>{res}</code>"


@app.route("/mark/<uuid:task_id>")
def mark(task_id) -> str:
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    res, debug_info = real_mark(
        g.ctx, uuid_text=str(task_id), mark=CLICK_DEFAULT_VALUES["mark"]["mark"]
    )
    txt = robust_json_dumps(res, sort_keys=True, indent=2, separators=(",<br>", ":"))
    logger.info(txt)

    return _engage_message(txt, debug_info)


def _engage_message(txt: str, debug_info: dict) -> str:
    last_two_df = urllize_df(debug_info["last_two_df"], ["task_uuid"])
    last_engage_dur = last_two_df["dt"].iloc[0] - last_two_df["dt"].iloc[1]
    last_two_df_html = last_two_df.to_html(escape=False)
    return f"<code>{txt}</code><br>{last_two_df_html}<br>{last_engage_dur}"


@app.route("/unmark")
def unmark() -> str:
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    res, debug_info = real_mark(
        g.ctx, uuid_text=MARK_UNSET_SYMBOL, mark=CLICK_DEFAULT_VALUES["mark"]["mark"]
    )
    txt = robust_json_dumps(res, sort_keys=True, indent=2, separators=(",<br>", ":"))
    logger.info(txt)

    return _engage_message(txt, debug_info)


@app.route("/edit/<uuid:task_id>", methods=["POST"])
def edit(task_id) -> str:
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)

    logger.info(f"ti: {task_id}")
    logger.info(f"form: {request.form}")
    form = dict(request.form)
    logger.info(f"form: {form}")

    for k, v in list(form.items()):
        if v == _NOTHING_TEXT_FORM_VALUE:
            form.pop(k)
        elif (k, v) == ("scheduled_date", _NONE_TEXT_FORM_VALUE):
            form[k] = _NONE_CLICK_VALUE
        elif (k, v) == ("due", _NONE_TEXT_FORM_VALUE):
            form[k] = _NONE_CLICK_VALUE
    form["tags"] = form.get("tags", "").split()
    logger.info(f"form: {form}")

    if len(form) > 0:
        kwargs = dict(
            scheduled_date=None,
            due=None,
            tags=[],
            uuid_text=[str(task_id)],
        )

        # if "status" in kwargs:
        #     form["status"] = form["status"]
        # if "scheduled_date" in form:
        #     kwargs["scheduled_date"] = form["scheduled_date"]
        kwargs = {**kwargs, **form}

        logger.info(f"kwargs: {kwargs}")

        real_edit(
            ctx=g.ctx,
            **kwargs,
        )
    else:
        kwargs = {}

    return json.dumps(kwargs)


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
    # logger.info(f"g: {my_g}")
    timings = {}

    with TimeItContext("init", report_dict=timings):
        gstasks_settings, mongo_url = _init()
        _init_g(g, mongo_url=mongo_url)
        gstasks_profiles = gstasks_settings["profiles"]
        gstasks_exe = gstasks_settings["gstasks_exe"]

    with TimeItContext("parse flask", report_dict=timings):
        args = request.args
        logger.info(f"args: {args}")
        args = request.args.to_dict()
        logger.info(f"args: {args}")

        profile = args.pop("profile") if "profile" in args else "standard"
        tag = args.pop("tag") if "tag" in args else None
        exclude_tag = args.pop("et") if "et" in args else None
        ## http://127.0.0.1:5000/ls?profile=ttask&bd=tomorrow&ad=tomorrow
        after_date = args.pop("ad") if "ad" in args else None
        before_date = args.pop("bd") if "bd" in args else None
        text = args.pop("text") if "text" in args else None

        logger.info(
            dict(
                args=args,
                profile=profile,
                tag=tag,
                after_date=after_date,
                before_date=before_date,
                exclude_tag=exclude_tag,
                text=text,
            )
        )

    with TimeItContext("widgets", report_dict=timings):
        gstasks_profile = gstasks_profiles[profile]
        jinja_env = {"widgets": {}}
        ## FIXME: reface: organise as classes with common superclass
        ## move to separate package
        for widget, widget_config in gstasks_profile.get("widgets", {}).items():
            if widget == "habits":
                jinja_env["widgets"]["habits_df"] = _get_habits(
                    str_or_envvar(widget_config["mongo_url"])
                    if "mongo_url" in widget_config
                    else None
                )
            elif widget == "tags":
                jinja_env["widgets"]["tags_df"] = WidgetTags(**widget_config)(
                    profile=profile
                )
            else:
                logger.error(dict(widget=widget))

    with TimeItContext("run", report_dict=timings):
        out_fns = {}
        keys = " ".join([f"-{k} {v}" for k, v in args.items()])
        kwargss = {}
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
                        exclude_tag=exclude_tag,
                        text=text,
                    )
                )
                logger.info(f"cmd: `{cmd}`")
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
                if text is not None:
                    kwargs["text"] = text
                if exclude_tag is not None:
                    kwargs["exclude_tags"] = [exclude_tag]
                for kk, vv in dict(
                    before_date=before_date, after_date=after_date
                ).items():
                    if vv is not None:
                        kwargs[kk] = vv
                logger.info(f"kwargs: {kwargs}")
                real_ls(**kwargs)
                kwargss[k] = kwargs
            out_fns[k] = out_fn

    with TimeItContext("read output", report_dict=timings):
        outs = {}
        for k, out_fn in out_fns.items():
            logger.info(out_fn)
            with open(out_fn) as f:
                out = f.read()
            outs[k] = out
        if "template" in gstasks_profile:
            with open(gstasks_profile["template"]) as f:
                template = f.read()
            jinja_env = {
                **jinja_env,
                **dict(
                    table_htmls=outs,
                    kwargss={
                        k_: dict(filter(lambda t: t[0] not in ["ctx"], v_.items()))
                        for k_, v_ in kwargss.items()
                    },
                    static_files={},
                    utils={
                        "safe_dump": lambda d: base64.urlsafe_b64encode(
                            json.dumps(d).encode()
                        ).decode()
                    },
                ),
            }
            for k, fn in gstasks_profile.get("static_files", {}).items():
                with open(fn) as f:
                    jinja_env["static_files"][k] = f.read()
            out = Template(template).render({**jinja_env})

    timings_df = pd.Series(timings).to_frame("duration_seconds")
    timings_df["dur"] = timings_df["duration_seconds"].apply(
        lambda s: timedelta(seconds=s)
    )
    timings_df["perc"] = timings_df["dur"] / timings_df["dur"].sum() * 100
    logger.info(timings_df)

    return out


@app.route("/download_json/<string:b64d>")
def download_json(b64d: str):
    # base64.urlsafe_b64encode(
    #                         json.dumps(d).encode()
    #                     ).decode()
    _, mongo_url = _init()
    _init_g(g, mongo_url=mongo_url)
    kwargs = json.loads(base64.urlsafe_b64decode(b64d.encode()).decode())
    logger.info(kwargs)
    out_file = f"/tmp/{uuid.uuid4()}.json"
    kwargs.pop("out_format_config")
    with open(out_file, "w") as f:
        kwargs = {**kwargs, "out_format": "json", "ctx": g.ctx, "click_echo": f.write}
        logger.info(kwargs)
        real_ls(**kwargs)
    return send_file(
        out_file,
        # attachment_filename=path.split(out_file)[1]
    )


class EchoToString:
    def __init__(self):
        self._s = ""

    def __call__(self, s):
        self._s = s

    @property
    def s(self):
        return self._s


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
