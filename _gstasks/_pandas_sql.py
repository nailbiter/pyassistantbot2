"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/pandas_sql.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION:

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION:
     VERSION: ---
     CREATED: 2022-09-06T23:28:48.574230
    REVISION: ---

==============================================================================="""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import sqlalchemy
import string

_UDF_PREF = "udf__"


def pandas_sql(
    sql, df_mapping, utils=[], engine_sqlalchemy_line="sqlite+pysqlite:///:memory:"
):
    """
    code sample: https://gist.github.com/nailbiter/d1dcdd76b013a064cb4de88246cef188
    @param `utils` -- [{callback:, name:, nargs: }]
    """
    engine = create_engine(engine_sqlalchemy_line)

    with engine.begin() as conn:
        for tn, df in df_mapping.items():
            df.to_sql(tn, conn, if_exists="replace", index=False)
    with engine.connect() as conn:
        #cur = conn.connection.cursor()
        for r in utils:
            # FIXME: will `conn.create_function` work??
            conn.connection.create_function(f'{_UDF_PREF}{r["name"]}', r["nargs"], r["callback"])
        df = pd.read_sql(text(sql), conn)
    return df
