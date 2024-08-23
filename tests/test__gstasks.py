"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/tests/test__gstasks.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-08-23T21:51:24.222907
    REVISION: ---

==============================================================================="""
import logging
import unittest
from _gstasks import next_work_day
from datetime import datetime


# class TestGstasks(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._logger = logging.getLogger(self.__class__.__name__)

#     def test_something(self):
#         self.assertTrue(1 == 1)
#         self.assertEqual(1, 1)
#         self.assertNotEqual(1, 2)


def test_next_work_day():
    assert next_work_day(datetime(2024, 8, 23)) == datetime(2024, 8, 26)
    assert next_work_day(datetime(2024, 8, 22)) == datetime(2024, 8, 23)
    assert next_work_day(datetime(2024, 8, 24)) == datetime(2024, 8, 26)
