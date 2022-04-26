"""===============================================================================

        FILE: _common/SimpleMathEval.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION:

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: adapted from https://www.tutorialspoint.com/program-to-evaluate-one-mathematical-expression-without-built-in-functions-in-python
                and https://stackoverflow.com/a/9558001
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION:
     VERSION: ---
     CREATED: 2021-12-29T13:51:16.035078
    REVISION: ---

==============================================================================="""
#from math import floor, trunc
import logging
import ast
import operator as op


def simple_math_eval(s, number_utils=(float, float), is_verbose=False):
    s = list(s[::-1])
    string_to_num, float_to_num = number_utils

    def get_value():
        sign = 1
        if s and s[-1] == "-":
            s.pop()
            sign = -1
        value = 0
        while s and s[-1].isdigit():
            value *= 10
            value += string_to_num(s.pop())
        return sign * value

    def get_term():
        term = get_value()
        while s and s[-1] in "*/":
            op = s.pop()
            value = get_value()
            if op == "*":
                term *= value
            else:
                term = float_to_num(1.0 * term / value)
        return term

    ans = get_term()
    while s:
        op, term = s.pop(), get_term()
        if is_verbose:
            logging.warning((ans, op, term))
        if op == "+":
            ans += term
        else:
            ans -= term
    return ans


# supported operators
operators = {ast.Add: op.add, ast.Sub: op.sub, ast.Mult: op.mul,
             ast.Div: op.truediv, ast.Pow: op.pow, ast.BitXor: op.xor,
             ast.USub: op.neg}


def eval_expr(expr):
    """
    >>> eval_expr('2^6')
    4
    >>> eval_expr('2**6')
    64
    >>> eval_expr('1 + 2*3**(4^5) / (6 + -7)')
    -5.0
    """
    return eval_(ast.parse(expr, mode='eval').body)


def eval_(node):
    if isinstance(node, ast.Num):  # <number>
        return node.n
    elif isinstance(node, ast.BinOp):  # <left> <operator> <right>
        return operators[type(node.op)](eval_(node.left), eval_(node.right))
    elif isinstance(node, ast.UnaryOp):  # <operator> <operand> e.g., -1
        return operators[type(node.op)](eval_(node.operand))
    else:
        raise TypeError(node)


if __name__ == "__main__":
    ob = Solution()
    s = "2+3*5/7"
    print(ob.solve(s))
