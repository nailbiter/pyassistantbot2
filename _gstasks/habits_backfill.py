"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/habits_backfill.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-08-09T21:42:45.666016
    REVISION: ---

==============================================================================="""
import croniter
from datetime import datetime, timedelta
import typing


def generate_habits_series(
    start: datetime, cronline: str, end: typing.Optional[datetime] = None
) -> list[datetime]:
    end = datetime.now() if end is None else end
    res = []
    iter = croniter.croniter(cronline, start)
    while (dt := iter.get_next(datetime)) <= end:
        res.append(dt)
    return res
