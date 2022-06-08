#!/usr/bin/env python3
"""===============================================================================

        FILE: nutrition.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-06-04T15:25:39.698977
    REVISION: ---

==============================================================================="""

import click
#from dotenv import load_dotenv
import os
from os import path
import logging

@click.command()
def nutrition():
    pass

if __name__=="__main__":
    if path.isfile(".env"):
        logging.warning("loading .env")
        load_dotenv()
    nutrition()
