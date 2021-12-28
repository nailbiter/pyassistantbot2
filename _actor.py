"""===============================================================================

        FILE: _actor.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-12-15T13:30:12.967895
    REVISION: ---

==============================================================================="""
import _common
import re
from datetime import datetime


def add_money(text, send_message_cb=None, mongo_client=None):
    amount, *other = re.split(r" +", text)
    amount = float(amount)
    tags = set()
    date = datetime.now()
    category = None
    for i, x in enumerate(other):
        if x.startswith("#"):
            x = x[1:]
            if x in ["food", "fun"]:
                category = x
            else:
                tags.add(x)
        elif x.startswith("%"):
            x = x[1:]
            if len(x) == 12:
                date = datetime.strptime(x, "%Y%m%d%H%M")
            elif len(x) == 6:
                date = datetime.strptime(x, "%d%H%M")
        else:
            break
    comment = " ".join(other[i:])
    assert category is not None
    mongo_client[_common.MONGO_COLL_NAME]["alex.money"].insert_one({
        "date": _common.to_utc_date(date),
        "comment": comment,
        "tags": sorted(list(tags)),
        "category": category,
        "amount": amount,
    })
    send_message_cb(f"added amount {amount} to category {category}")
