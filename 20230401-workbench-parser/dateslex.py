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

# List of token names.   This is always required
tokens = (
    "PLUS",
    "MINUS",
    "LPAREN",
    "RPAREN",
    "DATETIME",
    "TIMEDELTA",
)

# Regular expression rules for simple tokens
t_PLUS = r"\+"
t_MINUS = r"-"
t_LPAREN = r"\("
t_RPAREN = r"\)"

# A regular expression rule with some action code
def t_DATETIME(t):
    r'''(["']\d{4}-\d{2}-\d{2}["']|today|yesterday|tomorrow|now)'''
    # t.value = int(t.value)
    if t.value not in ["today", "yesterday", "tomorrow", "now"]:
        t.value = datetime.strptime(t.value[1:-1], "%Y-%m-%d")
    return t


def t_TIMEDELTA(t):
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
def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


# Build the lexer
lexer = lex.lex()
