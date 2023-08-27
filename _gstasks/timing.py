"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/timing.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: adapted from https://github.com/nailbiter/alex_leontiev_toolbox_python/blob/8582775746c3197c61364f4c6e458d493da2d3b7/alex_leontiev_toolbox_python/utils/__init__.py#L194
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-08-27T18:31:09.097286
    REVISION: ---

==============================================================================="""
import logging
import time
import typing
from datetime import datetime, timedelta


class TimeItContext:
    """
    FIXME: also implement as a decorator
    """

    def __init__(
        self,
        title: str,
        is_warning_on_start: bool = False,
        is_warning_on_end: bool = False,
        method: typing.Literal["time", "perf_counter"] = "time",
        report_dict: typing.Optional[dict] = None,
    ):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._title = title
        self._is_warning_on_start = is_warning_on_start
        self._is_warning_on_end = is_warning_on_end
        self._report_dict = report_dict
        self._method = method

    def _get_tick(self):
        return getattr(time, self._method)()

    def __enter__(self, *args, **kwargs):
        self._tic = self._get_tick()
        if self._is_warning_on_start:
            self._logger.warning(
                f'"{self._title}" started at {str(datetime.fromtimestamp(self._tic))}'
            )

    def __exit__(self, *args, **kwargs):
        self._toc = self._get_tick()
        if self._is_warning_on_end:
            self._logger.warning(
                f'"{self._title}" ended at {str(datetime.fromtimestamp(self._toc))}'
            )
        duration_seconds = self._toc - self._tic
        if self._report_dict is not None:
            self._report_dict[self._title] = duration_seconds
        self._logger.warning(
            f'"{self._title}" took {str(timedelta(seconds=duration_seconds))}'
        )
