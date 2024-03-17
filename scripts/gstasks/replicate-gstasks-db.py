#!/usr/bin/env python3
"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/scripts/gstasks/replicate-gstasks-db.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-03-17T16:19:59.805096
    REVISION: ---

==============================================================================="""

import click

# from dotenv import load_dotenv
import os
from os import path
import logging
import pymongo
import pandas as pd
from jinja2 import Template
import subprocess
import uuid


def run_cmd(cmd: str) -> None:
    logging.warning(f"> {cmd}")
    os.system(cmd)


@click.command()
@click.option(
    "-u", "--remote-mongo-url", required=True, type=str, help="GSTASKS_MONGO_URL"
)
@click.option("-d", "--database", "databases", multiple=True)
def replicate_gstasks_db(remote_mongo_url, databases):
    # remote_mongo_client = pymongo.MongoClient(remote_mongo_url)
    local_mongo_client = pymongo.MongoClient()

    # db_df = pd.DataFrame(list(remote_mongo_client.list_databases()))
    # logging.warning(db_df)
    # assert set(databases) <= set(db_df["name"]), (set(databases), set(db_df["name"]))

    parsed_mongo_url = pymongo.uri_parser.parse_uri(remote_mongo_url)
    logging.warning(parsed_mongo_url)

    # mongodump --gzip --out (random-fn.py) --username ... --password ... --uri='...' --db gstasks
    # mongorestore (random-fn.py -r) --gzip


if __name__ == "__main__":
    #    fn = ".env"
    #    if path.isfile(fn):
    #        logging.warning(f"loading `{fn}`")
    #        load_dotenv(dotenv_path=fn)
    replicate_gstasks_db()
