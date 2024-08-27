"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/task_list.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-08-27T13:56:40.267025
    REVISION: ---

==============================================================================="""
from pymongo import MongoClient
import logging
import pandas as pd
from _gstasks.base import _format_url, make_mongo_friendly
from datetime import datetime, timedelta
import typing
import uuid
import sys


class TaskList:
    def __init__(self, mongo_url, database_name, collection_name):
        self._mongo_client = MongoClient(mongo_url)
        self._database_name = database_name
        self._collection_name = collection_name
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mongo_url = mongo_url

    @property
    def mongo_url(self):
        return self._mongo_url

    def get_all_tasks(
        self,
        is_post_processing: bool = True,
        is_drop_hidden_fields: bool = True,
        tags: list[str] = [],
        exclude_tags: list[str] = [],
    ) -> pd.DataFrame:
        filter_ = {}
        if tags:
            filter_["tags"] = {"$all": tags}
        df = pd.DataFrame(self.get_coll().find(filter=filter_))
        self._logger.warning(exclude_tags)
        self._logger.warning(df["tags"].to_list()[-5:])
        for exclude_tag in exclude_tags:
            df = df[
                df["tags"].apply(lambda l: pd.isna(l).all() or (exclude_tag not in l))
            ]
        #        df = df.sort_values(by=["_insertion_date", "_id"])
        if is_drop_hidden_fields:
            df.drop(columns=[x for x in list(df) if x.startswith("_")], inplace=True)
        if is_post_processing:
            df.insert(1, "U", df.pop("URL").apply(_format_url))

        return df

    def _log(self, **kwargs):
        r = kwargs
        r["timestamp"] = datetime.now()
        self._logger.info(r)
        self.get_coll(collection_name="actions").insert_one(r)

    def get_task(
        self, uuid_text=None, index=None, get_all_tasks_kwargs: dict = {}
    ) -> (dict, int):
        assert sum([x is not None for x in [index, uuid_text]]) == 1
        ## FIXME: this should be cached
        df = self.get_all_tasks(is_post_processing=False, **get_all_tasks_kwargs)
        if index is not None:
            r = df.to_dict(orient="records")[index]
        elif uuid_text is not None:
            slice_ = [
                (i, r)
                for i, r in enumerate(df.to_dict(orient="records"))
                if r["uuid"].startswith(uuid_text)
            ]
            assert len(slice_) == 1, (uuid_text, slice_)
            index, r = slice_[0]
        return r, index

    def get_coll(self, collection_name=None):
        if collection_name is None:
            collection_name = self._collection_name
        res = self._mongo_client[self._database_name][collection_name]

        # res = res.with_options(
        #     codec_options=tz(
        #         CodecOptions_aware=True, tzinfo=pytz.timezone(_LOCAL_TZ_NAME)
        #     )
        # )

        return res

    def insert_or_replace_record(
        self,
        r,
        index=None,
        action_comment: typing.Optional[str] = None,
        dry_run: bool = False,
    ):
        action = "inserting" if index is None else "replacing"

        assert action in ["inserting", "replacing"]
        print(f"{action} {r}", file=sys.stderr)
        if action == "inserting":
            index = len(self.get_all_tasks())

        log_kwargs = {}
        if action == "replacing":
            log_kwargs["previous_r"] = self.get_coll().find_one({"uuid": r["uuid"]})

        if "uuid" not in r:
            r["uuid"] = str(uuid.uuid4())

        # FIXMe(done): separate `insertion_date` and `last_update_date`
        if action == "inserting":
            r["_insertion_date"] = datetime.now()
        elif action == "replacing":
            r["_insertion_date"] = log_kwargs["previous_r"]["_insertion_date"]
        r["_last_modification_date"] = datetime.now()

        r = make_mongo_friendly(r)

        self._log(action=action, r=r, action_comment=action_comment, **log_kwargs)
        if dry_run:
            self._logger.warning(f"dry run {r}")
        else:
            self.get_coll().replace_one(
                filter={"uuid": r["uuid"]}, replacement=r, upsert=True
            )
        print(r["uuid"])
        return r["uuid"]
