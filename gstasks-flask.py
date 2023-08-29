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
from flask import Flask, request, g
from dotenv import load_dotenv
import os
from os import path
import subprocess
import uuid
import logging
import json
from jinja2 import Template
from _gstasks.timing import TimeItContext
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__)
# my_g = {}

# @app.before_first_request
# def before_first_request():
#     g.test = 1


@app.route("/ls", methods=["GET"])
def hello_world():
    # logging.warning(f"g: {my_g}")
    timings = {}

    with TimeItContext("init", report_dict=timings):
        gstasks_exe = os.environ.get("GSTASKS_FLASK_GSTASKS_EXE", "./gstasks.py")
        gstasks_profiles_fn = os.environ.get(
            "GSTASKS_FLASK_GSTASKS_PROFILES", ".gstasks_flask_profiles.json"
        )

        with open(gstasks_profiles_fn) as f:
            gstasks_profiles = json.load(f)

    with TimeItContext("parse flask", report_dict=timings):
        args = request.args
        args = request.args.to_dict()
        profile = "standard"
        if "profile" in args:
            profile = args.pop("profile")
        keys = " ".join([f"-{k} {v}" for k, v in args.items()])
        logging.warning(dict(args=args, profile=profile))

    with TimeItContext("run", report_dict=timings):
        out_fn = f"/tmp/{uuid.uuid4()}.html"
        cmd = Template(gstasks_profiles[profile]["cmd"]).render(
            dict(gstasks_exe=gstasks_exe, keys=keys, out_fn=out_fn)
        )
        logging.warning(f"cmd: `{cmd}`")
        ec, out = subprocess.getstatusoutput(cmd)
        assert ec == 0, (cmd, ec, out)

    with TimeItContext("read output", report_dict=timings):
        with open(out_fn) as f:
            out = f.read()

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

    port = int(os.getenv("PORT", 5000))
    # my_g["test"] = 1
    app.run(host="0.0.0.0", port=port)
