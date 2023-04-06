"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/20230401-workbench-parser/dateslex-test.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-04-01T22:41:13.635494
    REVISION: ---

==============================================================================="""

if __name__ == "__main__":
    from dateslex import DatesLexer

    # Test it out
    datas = [
        '"2023-04-01"+1d',
        "now+1h",
        "today+1d",
        "||",
    ]

    # Give the lexer some input
    for data in datas:
        print(data)

        m = DatesLexer()
        m.build()
        m.lexer.input(data.strip())

        # Tokenize
        while True:
            tok = m.lexer.token()
            if not tok:
                break  # No more input
            print(tok)
        print("\n")
