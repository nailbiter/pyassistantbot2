#!/usr/bin/env python3
"""===============================================================================

        FILE: tag-ttask.py

       USAGE: ./tag-ttask.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-06-06T16:37:10.325363
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
import re
from _ttask_common import ttask as _ttask


_NONE_TAG = "NONE"


@click.command()
@click.option("--mongo-url", envvar="MONGO_URL", required=True)
@click.option("-t", "--tag", multiple=True)
def tag_ttask(mongo_url, tag):
    ttask_df, _ = _ttask(mongo_url, is_print=False)
    ttask_df["tags"] = ttask_df.content.apply(re.compile(r"#[^\s]+").findall)
    ttask_df.tags = ttask_df.tags.apply(sorted)
    if len(tag) == 0:
        ttask_df.tags = ttask_df.tags.apply(
            lambda l: l if len(l) > 0 else [_NONE_TAG])
        ttask_df = ttask_df.explode("tags")
        click.echo(ttask_df.groupby("tags").agg({"content": len}))
    else:
        tag = {f"#{t}" for t in tag}
        ttask_df.tags = ttask_df.tags.apply(set)
        ttask_df = ttask_df[ttask_df.tags.apply(lambda t:len(tag & t)) > 0]
        click.echo(ttask_df)


if __name__ == "__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    tag_ttask()
