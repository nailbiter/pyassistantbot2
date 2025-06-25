"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/tests/test__simple_math_eval.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-06-25T06:17:12.675754
    REVISION: ---

==============================================================================="""
import logging
import unittest
from _common.simple_math_eval import simple_math_eval


# class TestSimpleMathEval(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._logger = logging.getLogger(self.__class__.__name__)


#     def test_something(self):
#         self.assertTrue(1 == 1)
#         self.assertEqual(1, 1)
#         self.assertNotEqual(1, 2)
def test_simple_math_eval():
    assert simple_math_eval("46.76", is_verbose=True) == 46.76
