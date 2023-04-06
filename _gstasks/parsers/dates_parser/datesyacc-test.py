"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/20230401-workbench-parser/datesyacc-test.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-04-05T22:50:16.092992
    REVISION: ---

==============================================================================="""
if __name__=="__main__":
    from datesyacc import DatesParser
    from dateslex import DatesLexer
    from datetime import datetime, timedelta

    x = datetime.now() - timedelta(days=1)
    now = datetime.now()

    dl = DatesLexer(x=x, now=now)
    dl.build()
    lexer = dl.lexer
    parser = DatesParser(x=x, now=now).parser

    inputs = [
        "x==now",
        "now==now",
        "now==none",
        "now!=none",
        "(x==now) or (x==none) or (x==x)",
    ]

    for input_ in inputs:
        print((input_, parser.parse(input_, lexer=lexer)))


    # while True:
    #     try:
    #         s = raw_input("calc > ")
    #     except EOFError:
    #         break
    #     if not s:
    #         continue
    #     result = parser.parse(s)
    #     print(result)
