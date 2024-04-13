#!/usr/bin/env python3
"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/produce-docker.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-11-12T01:07:03.053418
    REVISION: ---

==============================================================================="""

import click

# from dotenv import load_dotenv
import os
from os import path
import logging
from jinja2 import Template
from glob import glob
import natsort


def copy_dir(dir_: str) -> str:
    files = {
        *glob(f"{dir_}/*.py"),
        *glob(f"{dir_}/**/*.py", recursive=True),
    }
    files = natsort.natsorted(files)
    logging.warning(files)
    dirs = natsort.natsorted({path.dirname(fn) for fn in files})
    return "\n".join(
        [
            *[f"RUN mkdir -p {dir_}" for dir_ in dirs],
            *[f"COPY {fn} {fn}" for fn in files],
        ]
    )


@click.command()
@click.option("-i", "--input-file", type=click.Path(allow_dash=True))
@click.option("-o", "--output-file", type=click.Path())
def produce_docker(input_file, output_file):
    with click.open_file(input_file) as f:
        tpl = Template(f.read())
    with open(output_file, "w") as f:
        f.write(tpl.render(dict(utils=dict(copy_dir=copy_dir))))


if __name__ == "__main__":
    #    fn = ".env"
    #    if path.isfile(fn):
    #        logging.warning(f"loading `{fn}`")
    #        load_dotenv(dotenv_path=fn)
    produce_docker()
