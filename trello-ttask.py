#!/usr/bin/env python3
"""===============================================================================

        FILE: fordatawise/non-reusable/ttask.py

       USAGE: ./fordatawise/non-reusable/ttask.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-10-18T13:44:25.761585
    REVISION: ---

==============================================================================="""

import click
from _common import run_trello_cmd
import json
import os
import pandas as pd
import tqdm


@click.command()
@click.option("--habits-list-id", default=os.environ["TRELLO_LIST_ID"])
@click.option("-n", "--task-name", multiple=True)
@click.option("-l", "--how-many-tasks-to-leave", type=int, default=1)
def ttask(habits_list_id, task_name, how_many_tasks_to_leave):
    data = run_trello_cmd(f"low get-cards-of-list {habits_list_id}")
    data = json.loads(data)
    df = pd.DataFrame(data)
    df = df[["name", "id", "shortUrl"]]
    df = df[[not name.endswith("*") for name in df.name]]

#    print(df)
    counts_df = pd.DataFrame([{"name": name, "cnt": len(slice_)}
                             for name, slice_ in df.groupby("name")])
    counts_df = counts_df.sort_values(
        by=["cnt", "name"], ascending=[False, True])
    print(counts_df)

    if len(task_name) > 0:
        assert set(task_name) <= set(df.name), (set(task_name), set(df.name))
    for tn in set(task_name):
        assert tn in list(df.name), (tn,
                                     sorted(list(df.name.unique())))
        _df = df.query(f"name==\"{tn}\"")
        assert how_many_tasks_to_leave is not None
        if how_many_tasks_to_leave>=0:
            for id_ in tqdm.tqdm(list(_df.id.iloc[:max(len(_df)-how_many_tasks_to_leave, 0)])):
                run_trello_cmd(f"low update-card {id_} --closed true")
        else:
            for id_ in tqdm.tqdm(list(_df.id)[:-how_many_tasks_to_leave]):
                run_trello_cmd(f"low update-card {id_} --closed true")


if __name__ == "__main__":
    ttask()
