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


TODO:
    1. states filter (sql?)
    2. (states) order (?sql)
    3. color based on state (only stateonly state??)
    4. text formatting
        a. contract links
    5. text styling
        b. bold overdue tasks
==============================================================================="""
import itertools
import json
import logging
from os import path
from datetime import datetime, timedelta
import json5
import typing
import pandas as pd
from jinja2 import Template
from string import Template as string_template
import typing
import functools
import hashlib
import importlib.util

# copycat to omit dependency on `alex_leontiev_toolbox_python`
from _gstasks._pandas_sql import pandas_sql


def ifnull(x, y, method: typing.Literal["isna"] = "isna", is_loud: bool = False):
    res = y if pd.isna(x) else x
    if is_loud:
        logging.warning(("ifnull", (x, y), res))
    return res


class _get_task_by_uuid:
    def __init__(self, task_list):
        self._coll = task_list.get_coll()
        self._cache = {}
        self._logger = logging.getLogger(self.__class__.__name__)

    def __call__(self, uuid_: str, is_report_cache_hitmisses: bool = False):
        if uuid_ not in self._cache:
            if is_report_cache_hitmisses:
                self._logger.warning(f'cache miss with "{uuid_}"')
            self._cache[uuid_] = self._coll.find_one({"uuid": uuid_})
        elif is_report_cache_hitmisses:
            self._logger.warning(f'cache hit with "{uuid_}"')
        res = self._cache[uuid_]

        # self._logger.warning(
        #     (
        #         "_get_task_by_uuid",
        #         uuid_,
        #         res,
        #     )
        # )

        return res


def _df_env(df):
    df = df.copy()
    df.reset_index(inplace=True)

    tags = df.pop("tags")
    tags_df = pd.DataFrame(
        data=itertools.chain(
            *[
                [{"uuid": uuid, "tag": tag} for tag in tags_]
                for uuid, tags_ in zip(df["uuid"], tags)
            ]
        ),
        columns=["uuid", "tag"],
    )

    if "label" in df.columns:
        labels_df = pd.DataFrame(
            data=[({} if pd.isna(label) else label) for label in df.pop("label")]
        )
    else:
        labels_df = pd.DataFrame()

    labels_df["uuid"] = df["uuid"]

    res = dict(tasks=df, tags=tags_df, labels=labels_df)
    #    for k, df in res.items():
    #        logging.warning(f"{k}:\n{df.head().to_string()}")
    return res


def get_last_engaged_task_uuid(task_list,mark='engage'):
    l = list(
        task_list.get_coll("engage").find({"mark": mark}).sort("dt", -1).limit(1)
    )
    if len(l) == 0:
        return None
    else:
        return l[0]["task_uuid"]


def format_html(df, html_out_config, task_list, print_callback=print, out_file=None):
    #    logging.warning(html_out_config)

    if html_out_config is None:
        print_callback(df.to_html())
        return

    with open(html_out_config) as f:
        config = json5.load(f)
    logging.warning(f"config: {config}")

    # index set
    df = df.copy()
    df.set_index("uuid", inplace=True)
    assert df.index.is_unique

    # filtering
    df.drop(columns=["_id"], inplace=True)
    env = {
        "now": datetime.now(),
        "last_engaged_task_uuid": get_last_engaged_task_uuid(task_list),
        "utils": {
            "pd": pd,
            "custom": {
                "ifnull": ifnull,
                "get_task_by_uuid": _get_task_by_uuid(task_list),
            },
        },
    }

    ## load UDFs
    udfs = []
    if "sql_udfs_file" in config:
        udfs_fn = path.abspath(config["sql_udfs_file"])
        logging.warning(f"udfs_fn: `{udfs_fn}`")
        ## adapted from https://stackoverflow.com/a/67692
        spec = importlib.util.spec_from_file_location("gstasks_sql_udfs", udfs_fn)
        foo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(foo)
        #logging.info(dir(foo))
        #logging.warning(foo.export_udfs)
        udfs.extend(foo.export_udfs)
    logging.warning(f"udfs: {udfs}")

    # sorting/filtering
    if "sorting_sql" in config:
        with open(config["sorting_sql"]) as f:
            tpl = f.read()
        logging.info(tpl)
        sql = Template(tpl).render(env)
        logging.info(sql)
        res = pandas_sql(sql, _df_env(df), utils=udfs)
        logging.info("\n" + res.to_csv(index=None))
        df = df.loc[res["uuid"].to_list()]

    # row styling
    if "row_styling_sql" in config:
        with open(config["row_styling_sql"]) as f:
            tpl = f.read()
        logging.info(tpl)
        sql = Template(tpl).render(env)
        logging.info(sql)
        res_df = pandas_sql(sql, _df_env(df))
        res_df.set_index("uuid", inplace=True)
        classes = res_df.loc[df.index, "class"].to_list()
    else:
        classes = None

    # _date_cols = ["_insertion_date", "_last_modification_date"]
    # for cn in _date_cols:
    #     df[cn] = df[cn].apply(
    #         lambda dt: "" if pd.isna(dt) else dt.strftime("%Y-%m-%d %H:%M")
    #     )
    logging.warning(df.dtypes)

    # TODO: col order via `config`
    if "output_columns" in config:
        logging.warning(f'output_columns: {config["output_columns"]}')
        rs = list(df.reset_index().to_dict(orient="records"))
        # logging.warning(f"rs: {rs[:5]}")
        df = pd.DataFrame(
            {
                output_column["column_name"]: _render_column(output_column, rs, env)
                for output_column in config["output_columns"]
            },
            index=df.index,
        )
        # df = df[[r["column_name"] for r in config["output_columns"]]]
        # for r in config["output_columns"]:
        #     jinja_tpl = r.get("jinja_tpl")
        #     if "jinja_tpl" in r:
        #         df[r["column_name"]] = df[r["column_name"]].apply(
        #             lambda x: Template(r["jinja_tpl"])
        #             .render(
        #                 {
        #                     **env,
        #                     "x": x,
        #                 }
        #             )
        #             .strip()
        #         )

    out_file = config.get("out_file") if out_file is None else out_file
    is_use_style = config.get("is_use_style", False)
    s = (
        _style_to_buf(buf=out_file, config=config, df=df, classes=classes)
        if is_use_style
        else df.to_html(buf=out_file, render_links=True)
    )
    logging.warning(f'html saved to "{out_file}"')
    if s is not None:
        print_callback(s)


def _render_column(output_column, rs, env):
    # logging.warning(f"_render_column in: {output_column, rs[:5]}")
    res = map(
        lambda r: Template(output_column.get("jinja_tpl", "{{r[column_name]}}"))
        .render(
            {
                **env,
                "r": r,
                "column_name": output_column["column_name"],
                "x": r.get(output_column["column_name"]),
            }
        )
        .strip(),
        rs,
    )
    res = list(res)
    # logging.warning(res)
    return res


def _style_to_buf(
    buf: typing.Optional[str],
    config: dict,
    df: pd.DataFrame,
    classes: typing.Optional[list[str]],
):
    # formatting
    # formatting via SQL? via CSS?
    # TODO(done): next -- optional formatting via class assignment
    # TODO(done): add optional css via `config`

    html_template = config.get("template")
    logging.warning(f"html tpl: {html_template}")

    style = df.style

    if classes is not None:
        assert len(classes) == len(df), (len(classes), len(df))
        style = style.set_td_classes(
            pd.DataFrame(
                data=[[cls] * len(df.columns) for cls in classes],
                columns=df.columns,
                index=df.index,
            )
        )

    to_html_kwargs = {}
    if html_template is not None:
        to_html_kwargs["doctype_html"] = True

    html = style.to_html(**to_html_kwargs)

    if html_template is not None:
        # FIXME: solve `pandas` html escape problem and switch to `jinja2`
        with open(html_template) as f:
            tpl = string_template(f.read())
        row_num, col_num = df.shape
        html = tpl.substitute(table_html=html, row_num=row_num, col_num=col_num)

    if buf is not None:
        with open(buf, "w") as f:
            f.write(html)
        logging.warning(f'save to "{buf}"')
