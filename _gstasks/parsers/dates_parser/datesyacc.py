"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/20230401-workbench-parser/datesyacc.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-04-01T23:02:04.392859
    REVISION: ---

==============================================================================="""

"""
datetime
timedelta
expression
"""

from _gstasks.parsers.dates_parser.dateslex import DatesLexer
import ply.yacc as yacc
from datetime import datetime, timedelta
import logging
import pandas as pd


class DatesParser(object):
    tokens = DatesLexer.tokens

    def __init__(self, x: datetime, now: datetime = None,is_loud:bool=False):
        if now is None:
            now = datetime.now()
        self._x = x
        self._now = now
        self.parser = yacc.yacc(module=self)
        self._is_loud = is_loud

    def p_expression_op(self, p):
        """expression : datetime E datetime
        | datetime NE datetime
        | datetime L datetime
        | datetime G datetime
        | datetime LE datetime
        | datetime GE datetime
        """
        if self._is_loud:
            logging.warning(("p_expression_op", list(p)))
        if p[2] == "==":
            p[0] = (p[1] == p[3]) if p[3] is not None else pd.isna(p[1])
        elif p[2] == "!=":
            p[0] = (p[1] != p[3]) if p[3] is not None else (not pd.isna(p[1]))
        elif p[2] == "<":
            p[0] = p[1] < p[3]
        elif p[2] == ">":
            p[0] = p[1] > p[3]
        elif p[2] == "<=":
            p[0] = p[1] <= p[3]
        elif p[2] == ">=":
            p[0] = p[1] >= p[3]

    def p_expression_ex(self, p):
        """expression : expression OR expression
        | expression AND expression"""
        
        if self._is_loud:
            logging.warning(("p_expression_ex", list(p)))
        if p[2] in ["&&", "and"]:
            p[0] = p[1] and p[3]
        elif p[2] in ["||", "or"]:
            p[0] = p[1] or p[3]

    def p_expression_bra(self, p):
        "expression : LPAREN expression RPAREN"

        if self._is_loud:
            logging.warning(("p_expression_bra", list(p)))
        p[0] = p[2]

    def p_datetime(self, p):
        "datetime : DATETIME"

        if self._is_loud:
            logging.warning(("p_datetime", list(p)))
        p[0] = p[1]

    def p_datetime_op(self, p):
        """
        datetime : datetime PLUS timedelta
                 | datetime MINUS timedelta
        """
        if self._is_loud:
            logging.warning(("p_datetime_op", list(p)))
        if p[2] == "+":
            p[0] = p[1] + p[3]
        elif p[3] == "-":
            p[0] = p[1] - p[3]

    def p_timedelta(self, p):
        "timedelta : TIMEDELTA"
        if self._is_loud:
            logging.warning(("p_timedelta", list(p)))
        p[0] = p[1]

    # def p_expression_plus(p):
    #     "expression : expression PLUS term"
    #     p[0] = p[1] + p[3]

    # def p_expression_minus(p):
    #     "expression : expression MINUS term"
    #     p[0] = p[1] - p[3]

    # def p_expression_term(p):
    #     "expression : term"
    #     p[0] = p[1]

    # def p_term_times(p):
    #     "term : term TIMES factor"
    #     p[0] = p[1] * p[3]

    # def p_term_div(p):
    #     "term : term DIVIDE factor"
    #     p[0] = p[1] / p[3]

    # def p_term_factor(p):
    #     "term : factor"
    #     p[0] = p[1]

    # def p_factor_num(p):
    #     "factor : NUMBER"
    #     p[0] = p[1]

    # def p_factor_expr(p):
    #     "factor : LPAREN expression RPAREN"
    #     p[0] = p[2]

    # Error rule for syntax errors
    def p_error(self, p):
        print("Syntax error in input!")


# Build the parser
# parser = yacc.yacc()
