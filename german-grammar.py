#!/usr/bin/env python3
"""===============================================================================

        FILE: german-grammar.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-04-18T21:35:06.864565
    REVISION: ---

==============================================================================="""

import click
from dotenv import load_dotenv
import os
from os import path
import logging
import pymongo
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
#import requests
import _common.requests_cache
#from lxml import html
import pandas as pd
from jinja2 import Template
import requests_cache

_PROCESSORS = {
    "prateritum": {
        "tpl": "https://www.verbformen.de/konjugation/indikativ/praeteritum/?w={{word}}",
        "sel": """#vVdBx > div.vTbl > table""",
    },
    "konjunktiv2": {
        "tpl": "https://de.pons.com/verbtabellen/deutsch/{{word}}",
        "sel": """section.pons:nth-child(5) > div:nth-child(4) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > table:nth-child(2)""",
    },
    "perfekt": {
        "tpl": "https://de.pons.com/verbtabellen/deutsch/{{word}}",
        "sel": """section.pons:nth-child(2) > div:nth-child(3) > div:nth-child(1) > div:nth-child(3) > div:nth-child(1) > table:nth-child(2)""",
    }
}


@click.command()
@click.option("--mongo-url", required=True, envvar="PYASSISTANTBOT_MONGO_URL")
@click.option("-t", "--type", "type_", type=click.Choice(_PROCESSORS), default="prateritum")
@click.argument("word")
def german_grammar(mongo_url, type_, word):
    logging.basicConfig(level='DEBUG')
    #    print(mongo_url)
    session = requests_cache.CachedSession(
        'demo_cache', expire_after=timedelta(days=1))

#    get = _common.requests_cache.RequestGet(10, ".german_grammar.db")
    get = session.get

    p = _PROCESSORS[type_]
    r = get(Template(p["tpl"]).render({"word": word}))
    status_code = r.status_code
    text = r.text
    assert status_code == 200, (status_code, text)
    soup = BeautifulSoup(text, 'html.parser')
#    logging.warning(text)

    with open("/tmp/d2b89983_24ae_4839_acc5_c5a05076028b.html", "w") as f:
        f.write(text)
    df, = pd.read_html(str(soup.select(p["sel"])))
    print(df)


if __name__ == "__main__":
    #    if path.isfile(".env"):
    #        logging.warning("loading .env")
    #        load_dotenv(override=True)
    german_grammar()
