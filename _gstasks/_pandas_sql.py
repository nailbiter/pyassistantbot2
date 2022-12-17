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


def pandas_sql(sql, df_mapping, engine_sqlalchemy_line="sqlite+pysqlite:///:memory:"):
    engine = create_engine(engine_sqlalchemy_line, future=True)

    with engine.begin() as conn:
        for tn, df in df_mapping.items():
            df.to_sql(tn, conn, if_exists="replace", index=False)
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    return df
