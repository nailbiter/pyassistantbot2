"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/labels_types.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-04-20T18:13:18.778456
    REVISION: ---

==============================================================================="""

import typing
import pandas as pd


class _DateLabel:
    @classmethod
    def is_validated(cls, s: str) -> bool:
        return pd.notna(pd.to_datetime(s, errors="coerce"))

    def __init__(self, s: str):
        pass

    def to_html(self) -> str:
        pass


LABELS_TYPES = dict(date=_DateLabel)
