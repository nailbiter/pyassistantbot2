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
    return None, "NO_BOTHER" or state
    """
    mongo_coll = mongo_client[MONGO_COLL_NAME]["alex.sleepingtimes"]
    last_record = mongo_coll.find_one(sort=[("startsleep",pymongo.DESCENDING)])
    if last_record.get("endsleep",None) is not None:
        return None
    else:
        return "NO_BOTHER" if last_record["category"]=="sleeping" else last_record["category"]
