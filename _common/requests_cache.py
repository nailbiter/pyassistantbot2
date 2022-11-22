"""===============================================================================

        FILE: _common/requests_cache.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-04-18T21:47:10.758307
    REVISION: ---

==============================================================================="""

import pandas as pd
import requests
import sqlite3
from datetime import datetime, timedelta
import json
import logging


_REQUEST_GET_TABLE_NAME = "requests_get"


class RequestGet:
    def __init__(self, cache_lifetime_min, cache_db, requests_kwargs={}):
        """
        @param cache_lifetime_min 0, -1 or >0 (0 means no cache, -1 means endless)
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        assert cache_lifetime_min >= 0 or cache_lifetime_min == -1
        self._cache_lifetime_min = cache_lifetime_min
        self._cache_db = cache_db
        self._requests_kwargs = requests_kwargs

    def _get_conn(self,):
        conn = sqlite3.connect(self._cache_db)
        return conn

    def _get_url(self, url):
        return requests.get(url, **self._requests_kwargs)

    def _r_to_json(self, r):
        status_code = r.status_code
        text = r.text
        r = {"status_code": status_code, "text": text}
        return r

    def __call__(self, url, is_force_cache_miss=False):
        """
        return (return_code, text)
        """
        if self._cache_lifetime_min == 0:
            # FIXME: should return 2 arguments to satisfy the interface contract
            return self._get_url(url)

        conn = self._get_conn()
        _NULL_ANSWER = pd.DataFrame([], columns=["reply_json", "datetime"])
        if is_force_cache_miss:
            df = _NULL_ANSWER
        else:
            try:
                df = pd.read_sql_query(
                    f'SELECT reply_json, datetime FROM {_REQUEST_GET_TABLE_NAME} where url="{url}" order by datetime desc', conn)
            except pd.io.sql.DatabaseError:
                df = _NULL_ANSWER

        df.datetime = df.datetime.apply(datetime.fromisoformat)
        df.reply_json = df.reply_json.apply(json.loads)
        now = datetime.now()
        if len(df) == 0 or ((now-df.datetime.iloc[0]).total_seconds()/60 >= self._cache_lifetime_min > 0):
            r = self._get_url(url)
            r = self._r_to_json(r)
            pd.DataFrame([
                {"datetime": now.isoformat(), "reply_json": json.dumps(r), "url": url}
            ]).to_sql(_REQUEST_GET_TABLE_NAME, conn, if_exists='append', index=None)
        else:
            active_for = str(
                now-df.datetime.iloc[0]) if self._cache_lifetime_min > 0 else "∞"
            self._logger.warning(
                f"use cache (active for {active_for})")
            r = df.reply_json.iloc[0]
        conn.close()
        return r["status_code"], r["text"]
