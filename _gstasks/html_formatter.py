"""===============================================================================

        FILE: _gstasks/html_formatter.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-12-16T20:37:31.068331
    REVISION: ---


TODO:
    1. states filter (sql?)
    2. (states) order (?sql)
    3. color based on state (only stateonly state??)
    4. text formatting
        a. contract links
    5. text styling
        b. bold overdue tasks
==============================================================================="""
import itertools
import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from jinja2 import Template

# copycat to omit dependency on `alex_leontiev_toolbox_python`
from _gstasks._pandas_sql import pandas_sql


def _df_env(df):
    df = df.copy()
    df.reset_index(inplace=True)

    tags = df.pop("tags")
    tags = pd.DataFrame(
        data=itertools.chain(
            *[
                [{"uuid": uuid, "tag": tag} for tag in tags_]
                for uuid, tags_ in zip(df["uuid"], tags)
            ]
        ),
        columns=["uuid", "tag"],
    )

    res = dict(tasks=df, tags=tags)
    #    for k, df in res.items():
    #        logging.warning(f"{k}:\n{df.head().to_string()}")
    return res


def format_html(df, html_out_config, print_callback=print):
    #    logging.warning(html_out_config)

    if html_out_config is None:
        print_callback(df.to_html())
        return

    with open(html_out_config) as f:
        config = json.load(f)
    logging.warning(f"config: {config}")

    # index set
    df = df.copy()
    df.set_index("uuid", inplace=True)
    assert df.index.is_unique

    # filtering
    df.drop(columns=["_id"], inplace=True)

    # sorting/filtering
    if "sorting_sql" in config:
        with open(config["sorting_sql"]) as f:
            tpl = f.read()
        logging.warning(tpl)
        sql = Template(tpl).render(
            {
                "now": datetime.now(),
                "util": {},
            }
        )
        logging.warning(sql)
        res = pandas_sql(sql, _df_env(df))

        df = df.loc[res["uuid"].to_list()]

    # formatting
    _date_cols = ["_insertion_date", "_last_modification_date"]
    for cn in _date_cols:
        df[cn] = df[cn].apply(
            lambda dt: "" if pd.isna(dt) else dt.strftime("%Y-%m-%d %H:%M")
        )

    out_file = config.get("out_file")
    s = df.to_html(buf=out_file, render_links=True)
    logging.warning(f'html saved to "{out_file}"')
    if s is not None:
        print_callback(s)
