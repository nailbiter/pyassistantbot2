"""===============================================================================

        FILE: _common/SimpleMathEval.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION:

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: adapted from https://www.tutorialspoint.com/program-to-evaluate-one-mathematical-expression-without-built-in-functions-in-python
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION:
     VERSION: ---
     CREATED: 2021-12-29T13:51:16.035078
    REVISION: ---

==============================================================================="""
#from math import floor, trunc


def simple_math_eval(s, number_utils=(float, float)):
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
        if op == "+":
            ans += term
        else:
            ans -= term
    return ans


if __name__ == "__main__":
    ob = Solution()
    s = "2+3*5/7"
    print(ob.solve(s))
