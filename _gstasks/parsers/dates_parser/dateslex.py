"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/20230401-workbench-parser/dateslex.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: adapted from https://www.dabeaz.com/ply/ply.html
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-04-01T22:39:05.109132
    REVISION: ---

==============================================================================="""

import ply.lex as lex
from datetime import datetime, timedelta
import logging


class DatesLexer(object):
    tokens = (
        "PLUS",
        "MINUS",
        "LPAREN",
        "RPAREN",
        "DATETIME",
        "TIMEDELTA",
        "L",
        "G",
        "LE",
        "GE",
        "E",
        "NE",
        # "VAR",
        "AND",
        "OR",
    )

    # List of token names.   This is always required
    # Regular expression rules for simple tokens
    t_PLUS = r"\+"
    t_MINUS = r"-"
    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_L = r"<"
    t_G = r">"
    t_LE = "<="
    t_GE = ">="
    t_E = "=="
    t_NE = "!="
    # t_VAR = "x"
    t_AND = r"(&&|and)"
    t_OR = r"(\|\||or)"

    # A regular expression rule with some action code
    def t_DATETIME(self, t):
        r"""(["']\d{4}-\d{2}-\d{2}["']|today|yesterday|tomorrow|now|none|x)"""
        # FIXME: add "next (mon|tue|wed|thu|sat|sun)..."
        # t.value = int(t.value)
        if t.value == "now":
            t.value = self._now
        elif t.value == "none":
            t.value = None
        elif t.value == "today":
            t.value = datetime.strptime(self._now.strftime("%Y-%m-%d"), "%Y-%m-%d")
        elif t.value == "yesterday":
            t.value = datetime.strptime(self._now.strftime("%Y-%m-%d"), "%Y-%m-%d")
            t.value -= timedelta(days=1)
        elif t.value == "tomorrow":
            t.value = datetime.strptime(self._now.strftime("%Y-%m-%d"), "%Y-%m-%d")
            t.value += timedelta(days=1)
        elif t.value == "x":
            t.value = self._x
        else:
            t.value = datetime.strptime(t.value[1:-1], "%Y-%m-%d")
        return t

    def t_TIMEDELTA(self, t):
        r"(?P<dur>\d+)(?P<unit>[dhm])?"

        # logging.warning((t.value, t.lexer.lexmatch))

        units = {k[0]: f"{k}s" for k in "day hour month".split()}
        units[None] = units["d"]

        unit = units[t.lexer.lexmatch.group("unit")]
        t.value = timedelta(**{unit: int(t.lexer.lexmatch.group("dur"))})
        return t

    # # Define a rule so we can track line numbers
    # def t_newline(t):
    #     r"\n+"
    #     t.lexer.lineno += len(t.value)

    # A string containing ignored characters (spaces and tabs)
    t_ignore = " \t"

    # Error handling rule
    def t_error(self, t):
        print("Illegal character '%s'" % t.value[0])
        t.lexer.skip(1)

    # Build the lexer
    def build(self, **kwargs):
        self.lexer = lex.lex(object=self, **kwargs)

    def __init__(self, x: datetime, now: datetime = None):
        if now is None:
            now = datetime.now()
        self._x = x
        self._now = now

    # lexer = lex.lex()
