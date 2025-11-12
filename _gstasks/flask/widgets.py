"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/flask/widgets.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-09-12T19:28:47.565981
    REVISION: ---

==============================================================================="""
import logging
import typing

import pandas as pd
import pymongo
from jinja2 import Template

from _gstasks import str_or_envvar, StringContractor
from _gstasks.my_logging import get_configured_logger


class WidgetTags:
    TAGS_WIDGET_TASK_FIELDS = ["task_name", "task_uuid"]

    def __init__(
        self,
        log_level: typing.Literal["INFO", "WARNING", "DEBUG"] = "INFO",
        maxlen: typing.Optional[int] = None,
        ellipsis_symbol: typing.Optional[str] = None,
        **widget_config,
    ):
        self.widget_config = widget_config

        self._contract: typing.Callable = (
            (lambda x: x)
            if maxlen is None
            else StringContractor(
                maxlen=maxlen,
                **{
                    **(
                        {}
                        if ellipsis_symbol is None
                        else dict(ellipsis_symbol=ellipsis_symbol)
                    ),
                },
            )
        )

        self._logger = get_configured_logger(self.__class__.__name__, level="DEBUG")

        self._logger.info(f"log_level: {log_level}")

    def __call__(self, profile: str, name: typing.Optional[str] = None) -> pd.DataFrame:
        widget_config = self.widget_config

        tags_df = pd.DataFrame(dict(tag_name=["tag"], cnt=[999]))
        mongo_client = pymongo.MongoClient(
            str_or_envvar(widget_config["mongo_url"])
            if "mongo_url" in widget_config
            else None
        )

        match_ = {}
        if "match_status" in widget_config:
            match_["status"] = widget_config["match_status"]
        elif "match_statuses" in widget_config:
            statuses = widget_config["match_statuses"]
            match_["status"] = {"$in": statuses}
        self._logger.info(f"match_: {match_}")

        _df = pd.DataFrame(
            mongo_client["gstasks"]["tasks"].aggregate(
                [
                    {"$match": match_},
                    {"$unwind": "$tags"},
                    {
                        "$group": {
                            "_id": "$tags",
                            "count": {"$sum": 1},
                            "due": {"$min": "$due"},
                            "task_name": {
                                "$top": {
                                    "sortBy": {"_last_modification_date": -1},
                                    "output": ["$name"],
                                }
                            },
                            "task_uuid": {
                                "$top": {
                                    "sortBy": {"_last_modification_date": -1},
                                    "output": ["$uuid"],
                                }
                            },
                        }
                    },
                ]
            )
        )
        for cn in [*WidgetTags.TAGS_WIDGET_TASK_FIELDS]:
            _df[cn] = _df[cn].apply(lambda l: None if len(l) == 0 else l[0])
        self._logger.debug(f"_df:\n{_df}")
        tags_df = (
            pd.DataFrame(mongo_client["gstasks"]["tags"].find())
            .drop(columns=["_id"])
            .merge(_df, how="inner", left_on="uuid", right_on="_id")[
                ["name", "count", "due", "task_name", "task_uuid"]
            ]
        )
        tags_df.rename(columns={"name": "tag_name"}, inplace=True)
        tags_df.set_index("tag_name", inplace=True)
        tag_names = widget_config.get("tags", [])
        if len(tag_names) > 0:
            tags_df = tags_df.loc[[x for x in tag_names if x in tags_df.index]]
        else:
            tags_df.sort_index(inplace=True)

        tpl = Template(
            widget_config.get(
                "tag_url_tpl",
                """<a href="ls?profile={{profile}}&tag={{name}}">{{name}}</a>""",
            )
        )
        tags_df.index = (
            tags_df.index.to_series()
            .apply(lambda name: tpl.render(dict(name=name, profile=profile)))
            .to_list()
        )

        tpl = Template(
            widget_config.get(
                "task_url_tpl",
                """<a href="lso/{{task_uuid}}">{{contract(task_name)}}</a>""",
            )
        )
        tags_df["task"] = tags_df[WidgetTags.TAGS_WIDGET_TASK_FIELDS].apply(
            lambda row: tpl.render(row.to_dict(), contract=self._contract), axis=1
        )
        tags_df.drop(columns=WidgetTags.TAGS_WIDGET_TASK_FIELDS, inplace=True)

        return tags_df
