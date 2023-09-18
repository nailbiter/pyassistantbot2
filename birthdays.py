#!/usr/bin/env python3
"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/birthdays.py

       USAGE: ./birthdays.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-09-18T19:41:28.150260
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
import pymongo
import json5
import pandas as pd
from datetime import datetime, timedelta


@click.command()
@click.option("--birthday-file", default="birthdays.json5", type=click.Path())
def birthdays(birthday_file):
    # mongo_client = pymongo.MongoClient(os.environ["MONGO_URL"])
    with open(birthday_file) as f:
        birthdays = json5.load(f)
    birthdays_df = pd.DataFrame(birthdays)
    click.echo(birthdays_df)


if __name__ == "__main__":
    # fn = ".env"
    # if path.isfile(fn):
    #     logging.warning(f"loading `{fn}`")
    #     load_dotenv(dotenv_path=fn)

    birthdays()
