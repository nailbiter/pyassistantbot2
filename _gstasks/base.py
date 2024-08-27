"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/base.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-08-27T13:58:07.679252
    REVISION: ---

==============================================================================="""
import pandas as pd


def _format_url(url) -> str:
    if not url:
        return ""
    elif pd.isna(url):
        return ""
    elif url.startswith("https://trello.com/c/"):
        return "T"
    else:
        return "U"


def make_mongo_friendly(r: dict) -> dict:
    # FIXME: why this happens?
    for k in ["due", "scheduled_date"]:
        if (k in r) and pd.isna(r[k]):
            r[k] = None
    return r
