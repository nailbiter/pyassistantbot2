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
from flask import Flask, request
from dotenv import load_dotenv
import os
from os import path
import subprocess
import uuid
import logging
import json
from jinja2 import Template

app = Flask(__name__)


@app.route("/ls", methods=["GET"])
def hello_world():
    gstasks_exe = os.environ.get("GSTASKS_FLASK_GSTASKS_EXE", "./gstasks.py")
    gstasks_profiles_fn = os.environ.get(
        "GSTASKS_FLASK_GSTASKS_PROFILES", ".gstasks_flask_profiles.json"
    )

    with open(gstasks_profiles_fn) as f:
        gstasks_profiles = json.load(f)

    args = request.args
    args = request.args.to_dict()
    profile = "standard"
    if "profile" in args:
        profile = args.pop("profile")
    keys = " ".join([f"-{k} {v}" for k, v in args.items()])
    logging.warning(dict(args=args, profile=profile))

    out_fn = f"/tmp/{uuid.uuid4()}.html"
    cmd = Template(gstasks_profiles[profile]).render(
        dict(gstasks_exe=gstasks_exe, keys=keys, out_fn=out_fn)
    )
    logging.warning(f"cmd: `{cmd}`")
    ec, out = subprocess.getstatusoutput(cmd)
    assert ec == 0, (cmd, ec, out)
    with open(out_fn) as f:
        out = f.read()
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
    app.run(host="0.0.0.0", port=port)
