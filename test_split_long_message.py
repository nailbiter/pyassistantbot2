"""===============================================================================

        FILE: test_split_long_message.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-03-28T10:39:53.356694
    REVISION: ---

==============================================================================="""
import logging
import unittest
from _common import split_long_text


class TestSplitLongMessage(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logging.getLogger(self.__class__.__name__)

    def test_something(self):
        assert split_long_text("a\nb\nc", 1) == ["a", "b", "c"]
        assert split_long_text("a\nb\nc", 3) == ["a\nb", "c"]
