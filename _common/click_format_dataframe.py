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

AVAILABLE_OUT_FORMATS = ["str", "csv", "json", "html", "plain", "csvfn"]


def format_df(df: pd.DataFrame, out_format: str, formatters: dict = {}) -> str:
    if out_format == "plain":
        s = str(df)
    elif out_format == "str":
        s = df.to_string()
    elif out_format == "json":
        s = df.to_json(orient="records")
    elif out_format == "csv":
        s = df.to_csv()
    elif out_format == "html":
        s = formatters.get("html", lambda df: df.to_html())(df)
        # logging.warning(f"{len(df)} tasks matched")
    elif out_format == "csvfn":
        raise NotImplementedError(dict(out_format=out_format))
    else:
        raise NotImplementedError(dict(out_format=out_format))

    return s

    # if out_format not in "json html csv".split():
    #    click.echo(f"{len(df)} tasks matched")
