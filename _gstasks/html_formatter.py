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

# FIXME
# copycat to omit dependency on `alex_leontiev_toolbox_python`
from _gstasks._pandas_sql import pandas_sql
from _gstasks import get_last_engaged_task_uuid, DEFAULT_JIRA_LABEL
from _gstasks.timing import TimeItContext


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


def _load_code_from_config_value(config_value):
    _FILE_NAME_PREFIX = "@"
    if config_value.startswith(_FILE_NAME_PREFIX):
        fn = config_value[len(_FILE_NAME_PREFIX) :]
        with open(fn) as f:
            tpl = f.read()
    else:
        tpl = config_value
    return tpl


def format_html(
    df, html_out_config=None, task_list=None, print_callback=print, out_file=None
):
    """
    TODO: timeit
    """
    #    logging.warning(html_out_config)

    assert task_list is not None
    assert html_out_config is not None

    timings = {}

    with TimeItContext("load html_out_config", report_dict=timings):
        if html_out_config is None:
            print_callback(df.to_html())
            return

        with open(html_out_config) as f:
            config = json5.load(f)
        logging.warning(f"config: {config}")

    with TimeItContext("index set", report_dict=timings):
        # index set
        df = df.copy()
        df.set_index("uuid", inplace=True)
        assert df.index.is_unique

    with TimeItContext("drop and env", report_dict=timings):
        # filtering
        df.drop(columns=["_id"], inplace=True)

        env = {
            "now": datetime.now(),
            "last_engaged_task_uuid": get_last_engaged_task_uuid(task_list),
            "utils": {
                "pd": pd,
                "json5": {"loads": json5.loads},
                "custom": {
                    "ifnull": ifnull,
                    "get_task_by_uuid": _get_task_by_uuid(task_list),
                },
            },
        }

    with TimeItContext("load udfs", report_dict=timings):
        ## load UDFs
        udfs = []
        if "sql_udfs_file" in config:
            udfs_fn = path.abspath(config["sql_udfs_file"])
            logging.warning(f"udfs_fn: `{udfs_fn}`")
            ## adapted from https://stackoverflow.com/a/67692
            spec = importlib.util.spec_from_file_location("gstasks_sql_udfs", udfs_fn)
            foo = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(foo)
            # logging.info(dir(foo))
            # logging.warning(foo.export_udfs)
            udfs.extend(foo.export_udfs)
        logging.warning(f"udfs: {udfs}")

    with TimeItContext("sorting/filtering", report_dict=timings):
        if "sorting_sql" in config:
            tpl = _load_code_from_config_value(config["sorting_sql"])
            logging.info(tpl)
            sql = Template(tpl).render(env)
            logging.info(sql)
            res = pandas_sql(sql, _df_env(df), utils=udfs)
            logging.info("\n" + res.to_csv(index=None))
            df = df.loc[res["uuid"].to_list()]

    with TimeItContext("row styling", report_dict=timings):
        if "row_styling_sql" in config:
            tpl = _load_code_from_config_value(config["row_styling_sql"])
            logging.info(tpl)
            sql = Template(tpl).render(env)
            logging.info(sql)
            res_df = pandas_sql(sql, _df_env(df))
            res_df.set_index("uuid", inplace=True)

            # classes = res_df.loc[df.index, "class"].to_list()
            class_fields = [cn for cn in res_df.columns if cn.startswith("class")]
            logging.warning(class_fields)
            assert len(class_fields) > 0
            classes = (
                res_df[class_fields]
                .apply(
                    lambda row: " ".join(
                        [x.strip() for x in row if len(x.strip()) > 0]
                    ),
                    axis=1,
                )
                .loc[df.index]
            )

            logging.warning(res_df)
        else:
            classes = None

    logging.warning(df.dtypes)

    with TimeItContext("columns styling", report_dict=timings):
        # FIXME: takes a long time
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

    with TimeItContext("output", report_dict=timings):
        out_file = config.get("out_file") if out_file is None else out_file
        is_use_style = config.get("is_use_style", False)
        df.index = [
            Template(config.get("index_style", "{{x}}")).render(dict(x=x, i=i))
            for i, x in enumerate(df.index)
        ]
        s = (
            _style_to_buf(buf=out_file, config=config, df=df, classes=classes)
            if is_use_style
            else df.to_html(buf=out_file, render_links=True)
        )
        logging.warning(f'html saved to "{out_file}"')

    with TimeItContext("print_callback", report_dict=timings):
        if s is not None:
            print_callback(s)

    timings_df = pd.Series(timings).to_frame("duration_seconds")
    timings_df["dur"] = timings_df["duration_seconds"].apply(
        lambda s: timedelta(seconds=s)
    )
    timings_df["perc"] = timings_df["dur"] / timings_df["dur"].sum() * 100
    logging.warning(timings_df)


def _render_column(output_column: dict, rs: list[dict], env: dict) -> list[str]:
    column_type = output_column.get("column_type", "usual")
    if column_type == "usual":
        xs = [r.get(output_column["column_name"]) for r in rs]
    elif column_type == "jira":
        # TODO
        df = pd.DataFrame(rs)
        df.loc[df["label"]]
        raise NotImplementedError(dict(column_type=column_type))
    else:
        raise NotImplementedError(dict(column_type=column_type))

    if "jinja_tpl" in output_column:
        tpl = Template(output_column["jinja_tpl"])
        return [
            tpl.render(
                {
                    **env,
                    "r": r,
                    "i": i,
                    "column_name": output_column["column_name"],
                    "x": x,
                }
            ).strip()
            for i, (r, x) in enumerate(zip(rs, xs))
        ]
    else:
        return xs


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

    if (classes is not None) and len(df) > 0:
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
        # with open(html_template) as f:
        #    tpl = string_template(f.read())
        tpl = string_template(_load_code_from_config_value(html_template))
        row_num, col_num = df.shape
        html = tpl.substitute(table_html=html, row_num=row_num, col_num=col_num)

    if buf is not None:
        with open(buf, "w") as f:
            f.write(html)
        logging.warning(f'save to "{buf}"')
