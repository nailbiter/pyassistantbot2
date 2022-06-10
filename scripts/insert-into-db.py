#!/usr/bin/env python3
"""===============================================================================

        FILE: scripts/insert-into-db.py

       USAGE: ./scripts/insert-into-db.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-06-08T19:22:29.854018
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
import pymongo
import pandas as pd
from datetime import datetime


@click.command()
@click.option("-f", "--file-path", type=click.Path(), required=True)
@click.option("-d", "--database", required=True)
@click.option("-c", "--collection", required=True)
@click.option("-n", "--column-name", required=True)
@click.option("-k", "--key-value", multiple=True, type=(str, str))
@click.option("-t", "--datetime-column-name")
@click.option("--mongo-url", envvar="PYASSISTANTBOT_MONGO_URL", required=True)
def insert_into_db(file_path, database, collection, column_name, key_value, datetime_column_name, mongo_url):
    with open(file_path) as f:
        df = f.readlines()
    df = [s.strip() for s in df]
    df = pd.DataFrame({column_name: df})
    for k, v in key_value:
        df[k] = v
    if datetime_column_name is not None:
        df[datetime_column_name] = datetime.now()
    pymongo.MongoClient(mongo_url)[database][collection].insert_many(
        df.to_dict(orient="records"))


if __name__ == "__main__":
    if path.isfile(".envrc"):
        logging.warning("loading .env")
        load_dotenv(override=True)
    insert_into_db()
