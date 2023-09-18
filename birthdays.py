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
from dateutil.relativedelta import relativedelta
import operator


@click.command()
@click.option(
    "--birthday-file", default="birthdays.json5", type=click.Path(exists=True)
)
def birthdays(birthday_file):
    # mongo_client = pymongo.MongoClient(os.environ["MONGO_URL"])
    with open(birthday_file) as f:
        birthdays = json5.load(f)
    birthdays_df = pd.DataFrame(birthdays)

    now = datetime.now()
    birthdays_df["date"] = [
        datetime(year=now.year, month=m, day=d)
        for m, d in zip(birthdays_df.pop("month"), birthdays_df.pop("date"))
    ]
    birthdays_df.loc[birthdays_df["date"] < now, "date"] = birthdays_df.loc[
        birthdays_df["date"] < now, "date"
    ].apply(operator.methodcaller("replace", year=now.year + 1))

    birthdays_df["days_remains"] = (birthdays_df["date"] - now).apply(
        # operator.methodcaller("total_seconds")
        operator.attrgetter("days")
    )
    # // (60 * 60 * 24)
    birthdays_df.sort_values(by="days_remains", inplace=True)

    click.echo(birthdays_df)


if __name__ == "__main__":
    # fn = ".env"
    # if path.isfile(fn):
    #     logging.warning(f"loading `{fn}`")
    #     load_dotenv(dotenv_path=fn)

    birthdays()
