"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/additional_states.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: FIXME: make it prettier (e.g. don't use code exec at package load time)
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-12-16T22:51:35.278172
    REVISION: ---

==============================================================================="""

import json
import logging
from os import path

_ADDITIONAL_STATES_FILE = path.join(path.dirname(__file__), ".additional_states.json")

ADDITIONAL_STATES = []
if path.isfile(_ADDITIONAL_STATES_FILE):
    with open(_ADDITIONAL_STATES_FILE) as f:
        ADDITIONAL_STATES = json.load(f)
