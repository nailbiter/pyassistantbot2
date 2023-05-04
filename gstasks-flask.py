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

app = Flask(__name__)


@app.route("/ls", methods=["GET"])
def hello_world():
    gstasks_exe = os.environ.get("GSTASKS_FLASK_GSTASKS_EXE", "./gstasks.py")

    args = request.args
    keys = " ".join([f"-{k} {v}" for k, v in request.args.to_dict().items()])
    logging.warning(f"args: {args}")

    out_fn = f"/tmp/{uuid.uuid4()}.html"
    cmd = f"{gstasks_exe} ls -o html -b today {keys} --out-file {out_fn}"
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
