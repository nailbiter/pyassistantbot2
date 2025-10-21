"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/typer.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-10-20T19:51:05.456940
    REVISION: ---

==============================================================================="""

import functools
import typing


@functools.singledispatch
def typeme(x) -> typing.Optional[str]:
    return None


@typeme.register
def _(x: str) -> typing.Optional[str]:
    return "str"


@typeme.register
def _(x: dict) -> typing.Optional[str]:
    return "dict"
