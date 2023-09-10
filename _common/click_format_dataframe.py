"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_common/click_format_dataframe.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-08-07T21:00:04.740725
    REVISION: ---

==============================================================================="""
import pandas as pd
import operator
from _common import get_random_fn
import logging

logging.warning(
    "module `click_format_dataframe` is DEPRECATED ==> switch to `alex_leontiev_toolbox_python.utils.click_format_dataframe`"
)

AVAILABLE_OUT_FORMATS = ["str", "csv", "json", "html", "plain", "csvfn"]

_DEFAULT_FORMATTERS = {
    "html": operator.methodcaller("to_html"),
}


def format_df(df: pd.DataFrame, out_format: str, formatters: dict = {}) -> str:
    formatters = {**_DEFAULT_FORMATTERS, **formatters}
    if out_format == "plain":
        s = str(df)
    elif out_format == "str":
        s = df.to_string()
    elif out_format == "json":
        s = df.to_json(
            orient="records",  # force_ascii=False
        )
    elif out_format == "csv":
        s = df.to_csv()
    elif out_format == "html":
        s = formatters[out_format](df)
        # logging.warning(f"{len(df)} tasks matched")
    elif out_format == "csvfn":
        s = get_random_fn(".csv")
        df.to_csv(s)
    else:
        raise NotImplementedError(dict(out_format=out_format))

    return s

    # if out_format not in "json html csv".split():
    #    click.echo(f"{len(df)} tasks matched")
