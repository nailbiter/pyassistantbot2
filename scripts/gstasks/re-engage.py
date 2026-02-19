#!/usr/bin/env python3
"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/scripts/gstasks/re-engage.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2026-02-18T21:20:59.913330
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
import functools
import subprocess
from typing import Optional


moption = functools.partial(click.option, show_envvar=True, show_default=True)


def run_cmd(cmd: str, capture_output: bool = False) -> Optional[str]:
    logger = logging.getLogger("run_cmd")
    logger.setLevel(logging.DEBUG)

    logger.debug(f"> {cmd}")
    res = subprocess.run(
        cmd,
        shell=True,
        check=True,
        stdout=subprocess.PIPE if capture_output else None,
        # stderr=subprocess.STDOUT if capture_output else None,
        text=True,
    )
    return res.stdout if capture_output else None


@click.command()
@click.option(
    "--gstasks-exe", default="./gstasks.py", envvar="GSTASKS_EXE", required=True
)
def re_engage(gstasks_exe):
    logger = logging.getLogger("re_engage")
    logger.setLevel(logging.INFO)

    res = run_cmd(f"{gstasks_exe} mark --is-out", capture_output=True)
    uuid_of_currently_engaged_task = res.strip()
    logger.info(f"\n```\n{uuid_of_currently_engaged_task}\n```")


if __name__ == "__main__":
    fn = ".env"
    if path.isfile(fn):
        logging.warning(f"loading `{fn}`")
        load_dotenv(dotenv_path=fn)
    re_engage()
