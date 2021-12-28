"""===============================================================================

        FILE: common.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-12-14T10:50:12.918934
    REVISION: ---

==============================================================================="""
import pymongo
from datetime import datetime, timedelta

TIME_CATS = [
    "sleeping",
    "logistics",
    "reading",
    "rest",
    "useless",
    "coding",
    "parttime",
    "gym",
    "work",
    "social",
    "german",
    "math project",
]

#MONGO_COLL_NAME = "pyassistantbot2"
MONGO_COLL_NAME = "logistics"


def get_sleeping_state(mongo_client):
    """
    return None or (is_no_bother,state)
    """
    mongo_coll = mongo_client[MONGO_COLL_NAME]["alex.sleepingtimes"]
    last_record = mongo_coll.find_one(
        sort=[("startsleep", pymongo.DESCENDING)])
    if last_record.get("endsleep", None) is not None:
        return None
    else:
        cat = last_record["category"]
        return cat == "sleeping", cat


def to_utc_date(date=datetime.now()):
    return date-timedelta(hours=9)
