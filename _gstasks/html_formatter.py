"""===============================================================================

        FILE: _gstasks/html_formatter.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-12-16T20:37:31.068331
    REVISION: ---

==============================================================================="""
import json
import logging


def format_html(df, html_out_config, print_callback=print):
    #    logging.warning(html_out_config)

    if html_out_config is None:
        print_callback(df.to_html())
        return

    with open(html_out_config) as f:
        config = json.load(f)
    logging.warning(f"config: {config}")

    out_file = config.get("out_file")

    df_html = df.to_html()

    if out_file is not None:
        df.to_html(out_file)
    else:
        print_callback(df.to_html())
