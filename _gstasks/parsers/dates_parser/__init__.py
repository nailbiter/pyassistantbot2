"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/parsers/dates_parser/__init__.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-04-06T22:00:10.194256
    REVISION: ---

==============================================================================="""
from _gstasks.parsers.dates_parser.datesyacc import DatesParser
from _gstasks.parsers.dates_parser.dateslex import DatesLexer
from datetime import datetime, timedelta
import functools


class DatesQueryEvaluator(object):
    def __init__(self, query):
        self._query = query
        self._now = datetime.now()
        self._cache = {}

    def __call__(self, x):
        if x not in self._cache:
            dl = DatesLexer(x=x, now=self._now)
            dl.build()
            lexer = dl.lexer
            parser = DatesParser(x=x, now=self._now).parser
            self._cache[x] = parser.parse(self._query, lexer=lexer)
        return self._cache[x]

    # x = datetime.now() - timedelta(days=1)
    # now = datetime.now()

    # inputs = [
    #     "x==now",
    #     "now==now",
    #     "now==none",
    #     "now!=none",
    #     "(x==now) or (x==none) or (x==x)",
    # ]

    # for input_ in inputs:
    #     print((input_, parser.parse(input_, lexer=lexer)))
