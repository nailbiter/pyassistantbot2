"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/gstasks_sql_uds.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-07-16T13:03:28.529032
    REVISION: ---

==============================================================================="""

import hashlib

export_udfs = [
    dict(
        name="md5",
        nargs=1,
        callback=lambda t: hashlib.md5(t.encode()).hexdigest(),
    )
]
