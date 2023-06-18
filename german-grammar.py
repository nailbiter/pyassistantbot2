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

import logging
import os
import re
from datetime import datetime, timedelta
from os import path

import click

# from lxml import html
import pandas as pd
import pymongo
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from jinja2 import Template
from lxml import etree, html

# import requests
import _common.requests_cache

_PROCESSORS = {
    "prateritum": {
        "tpl": "https://www.verbformen.de/konjugation/indikativ/praeteritum/?w={{word}}",
        "sel": ["""#vVdBx > div.vTbl > table"""],
        "method": "css-select",
    },
    "konjunktiv1": {
        "tpl": "https://www.verbformen.de/konjugation/{{word}}.htm",
        "sel": ["/html/body/article/div[1]/div[2]/div/section[2]/div[2]/div[4]"],
        "method": "xpath",
    },
    "konjunktiv2-vergangenheit": {
        "tpl": "https://de.pons.com/verbtabellen/deutsch/{{word}}",
        "sel": [
            """section.pons:nth-child(5) > div:nth-child(4) > div:nth-child(1) > div:nth-child(2) > div:nth-child(1) > table:nth-child(2)"""
        ],
        "method": "css-select",
    },
    "konjunktiv2": {
        "tpl": "https://de.pons.com/verbtabellen/deutsch/{{word}}",
        "sel": [
            """section.pons:nth-child(5) > div:nth-child(4) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > table:nth-child(2)"""
        ],
        "method": "css-select",
    },
    "perfekt": {
        "tpl": "https://de.pons.com/verbtabellen/deutsch/{{word}}",
        "sel": [
            """section.pons:nth-child(2) > div:nth-child(3) > div:nth-child(1) > div:nth-child(3) > div:nth-child(1) > table:nth-child(2)"""
        ],
        "method": "css-select",
    },
    "partizip2": {
        "tpl": "https://www.verbformen.de/konjugation/{{word}}.htm",
        "sel": [
            "/html/body/article/div[1]/div[2]/div/section[7]/div[2]/div[4]/table",
            "/html/body/article/div[1]/div[4]/div/section[7]/div[2]/div[4]/table",
        ],
        "method": "xpath",
    },
}


class FetchElementException(Exception):
    pass


def fetch_element(text, sel, method):
    assert method in "xpath css-select".split(), method
    if method == "css-select":
        soup = BeautifulSoup(text, "html.parser")
        return str(soup.select(sel))
    elif method == "xpath":
        try:
            el = html.fromstring(text)
            els = el.xpath(sel)
            assert len(els) > 0
            return "\n".join([etree.tostring(el).decode() for el in els])
        except (AssertionError, etree.ParserError) as e:
            raise FetchElementException(e)
    else:
        raise NotImplementedError(method)


@click.command()
@click.option("--mongo-url", required=True, envvar="PYASSISTANTBOT_MONGO_URL")
@click.option(
    "-t",
    "--type",
    "types",
    type=click.Choice(_PROCESSORS),
    default=["prateritum"],
    multiple=True,
)
@click.argument("word")
@click.option("-c", "--cache-lifetime-min", type=int, default=-1)
@click.option("--force-cache-miss/--no-force-cache-miss", "-f/ ", default=False)
@click.option(
    "-o", "--output-format", type=click.Choice("def json".split()), default="def"
)
@click.option("-i", "--df-index", type=int, default=0)
def german_grammar(
    mongo_url,
    types,
    word,
    cache_lifetime_min,
    force_cache_miss,
    output_format,
    df_index,
):
    get = _common.requests_cache.RequestGet(
        cache_lifetime_min,
        ".german_grammar.db",
    )
    for type_ in types:
        p = _PROCESSORS[type_]
        url = Template(p["tpl"]).render({"word": word})
        logging.warning(f'url: "{url}"')
        res = get(
            url,
            is_force_cache_miss=force_cache_miss,
        )
        #    logging.warning(res)
        status_code, text = res
        assert status_code == 200, (text, status_code, url)

        with open("/tmp/d2b89983_24ae_4839_acc5_c5a05076028b.html", "w") as f:
            f.write(text)
        for sel in p["sel"]:
            try:
                text = fetch_element(text, sel, p["method"])
                break
            except FetchElementException:
                logging.warning(f'attempt with sel="{sel}" failed')
                pass
        with open("/tmp/BD5C777D-FDBB-45BF-AF39-37266D20E1BE.html", "w") as f:
            f.write(text)
        dfs = pd.read_html(text)

        if len(dfs) > 1:
            logging.warning((dfs, len(dfs)))
        df = dfs[df_index]

        if output_format == "def":
            click.echo(df)
        elif output_format == "json":
            click.echo(df.to_json(force_ascii=False))


if __name__ == "__main__":
    #    if path.isfile(".env"):
    #        logging.warning("loading .env")
    #        load_dotenv(override=True)
    german_grammar()
