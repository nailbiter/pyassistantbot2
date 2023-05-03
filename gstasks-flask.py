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

app = Flask(__name__)


@app.route("/ls",methods=['GET'])
def hello_world():
    cmd = "./gstasks.py ls -o html -b today"
    return "Hello, World!"


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
