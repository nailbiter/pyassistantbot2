#!/usr/bin/env python3
"""===============================================================================

        FILE: forhabits/kostil/calorie-mate.py

       USAGE: ./forhabits/kostil/calorie-mate.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-06-24T18:25:33.559884
    REVISION: ---

==============================================================================="""

import click
from alex_python_toolbox import google_drive

@click.command()
def calorie_mate():
    creds = google_drive.get_creds("../../credentials_google_spreadsheet.json",create_if_not_exist=True)
    class_df = google_drive.download_df_from_google_sheets(
        creds, "1fgrbBlrMeAvOy3A8EhwHurZdXf9e7C6_p9yC1MwqBkA")
    click.echo(class_df)


if __name__=="__main__":
    calorie_mate()
