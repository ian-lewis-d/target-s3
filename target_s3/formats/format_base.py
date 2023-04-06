import re

import boto3
import inflection
import json
import collections
import logging
from datetime import datetime
from abc import ABCMeta, abstractmethod

from boto3 import Session
from smart_open import open


LOGGER = logging.getLogger("target-s3")
DATE_GRAIN = {
    "year": 7,
    "month": 6,
    "day": 5,
    "hour": 4,
    "minute": 3,
    "second": 2,
    "microsecond": 1,
}
COMPRESSION = {}


def format_type_factory(object_type_class, *pargs, **kargs):
    """A factory for creating ObjectTypes."""
    return object_type_class(*pargs, **kargs)


class FormatBase(metaclass=ABCMeta):

    """This is the object type base class"""

    def __init__(self, config: dict, context: dict, extension: str) -> None:
        # TODO: perhaps we should do some scrubbing here?
        self.config = config
        self.context = context
        self.extension = extension
        self.compression = "gz"  # TODO: need a list of compatible compression types

        self.stream_name_path_override = config.get("stream_name_path_override", None)

        self.bucket = config.get("bucket")  # required
        self.endpoint_url = config.get("endpoint_url", None)
        self.region_name = config.get("aws_region")
        self.aws_access_key = config.get("aws_access_key")
        self.aws_secret_access_key = config.get("aws_secret_access_key")
        self.aws_session_token = config.get("aws_session_token")
        self.aws_profile = config.get("aws_profile")

        self.prefix = config.get("prefix", None)
        self.logger = context["logger"]
        self.fully_qualified_key = self.create_key()
        self.logger.info(f"key: {self.fully_qualified_key}")

        # Create the session and s3_client
        if self.aws_access_key and self.aws_secret_access_key:
            self.aws_session = boto3.session.Session(
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token
            )
        else:
            self.aws_session = boto3.session.Session(profile_name=self.aws_profile)

        # Here we create the s3_client used to access the resources
        if self.endpoint_url:
            self.s3_client = boto3.client("s3", endpoint_url=self.endpoint_url)
        else:
            self.s3_client = boto3.client("s3")

    @abstractmethod
    def _write(self, contents: str = None) -> None:
        """Execute the write to S3. (default)"""
        # TODO: create dynamic cloud
        # TODO: is there a better way to handle write contents ?
        # Use s3_client.upload_fileobj to send the data as a bytestring
        self.s3_client.upload_fileobj(contents.encode(),
                                      self.bucket,
                                      f"{self.fully_qualified_key}.{self.extension}.{self.compression}"
                                      )

    @abstractmethod
    def run(self, records) -> None:
        """Execute the steps for preparing/writing records to S3. (default)"""
        self.records = records
        # prepare records for writing
        self._prepare_records()
        # write records to S3
        self._write()

    @abstractmethod
    def _prepare_records(self) -> None:
        """Execute record prep. (default)"""
        if self.config.get("include_process_date", None):
            self.records = self.append_process_date(self.records)
        if self.config.get("flatten_records", None):
            # flatten records
            self.records = list(
                map(lambda record: self.flatten_record(record), self.records)
            )

    def create_key(self) -> str:
        batch_start = self.context["batch_start_time"]
        stream_name = (
            self.context["stream_name"]
            if self.stream_name_path_override is None
            else self.stream_name_path_override
        )
        folder_path = f"{self.prefix}/{stream_name}/"
        file_name = ""
        if self.config["append_date_to_prefix"]:
            grain = DATE_GRAIN[self.config["append_date_to_prefix_grain"].lower()]
            folder_path += self.create_folder_structure(batch_start, grain)
        if self.config["append_date_to_filename"]:
            grain = DATE_GRAIN[self.config["append_date_to_filename_grain"].lower()]
            file_name += f"{self.create_file_structure(batch_start, grain)}"

        return f"{folder_path}{file_name}"

    def create_folder_structure(self, batch_start: datetime, grain: int) -> str:
        ret = ""
        ret += f"{batch_start.year}/" if grain <= 7 else ""
        ret += f"{batch_start.month:02}/" if grain <= 6 else ""
        ret += f"{batch_start.day:02}/" if grain <= 5 else ""
        ret += f"{batch_start.hour:02}/" if grain <= 4 else ""
        ret += f"{batch_start.minute:02}/" if grain <= 3 else ""
        ret += f"{batch_start.second:02}/" if grain <= 4 else ""
        ret += f"{batch_start.microsecond}/" if grain <= 1 else ""
        return ret

    def create_file_structure(self, batch_start: datetime, grain: int) -> str:
        ret = ""
        ret += f"{batch_start.year}" if grain <= 7 else ""
        ret += f"{batch_start.month:02}" if grain <= 6 else ""
        ret += f"{batch_start.day:02}" if grain <= 5 else ""
        ret += f"-{batch_start.hour:02}" if grain <= 4 else ""
        ret += f"{batch_start.minute:02}" if grain <= 3 else ""
        ret += f"{batch_start.second:02}" if grain <= 4 else ""
        ret += f"{batch_start.microsecond}" if grain <= 1 else ""
        return ret

    def flatten_key(self, k, parent_key, sep) -> str:
        """"""
        # TODO: standardize in the SDK?
        full_key = parent_key + [k]
        inflected_key = [n for n in full_key]
        reducer_index = 0
        while len(sep.join(inflected_key)) >= 255 and reducer_index < len(
            inflected_key
        ):
            reduced_key = re.sub(
                r"[a-z]", "", inflection.camelize(inflected_key[reducer_index])
            )
            inflected_key[reducer_index] = (
                reduced_key
                if len(reduced_key) > 1
                else inflected_key[reducer_index][0:3]
            ).lower()
            reducer_index += 1

        return sep.join(inflected_key)

    def flatten_record(self, d, parent_key=[], sep="__") -> dict:
        """"""
        # TODO: standardize in the SDK?
        items = []
        for k in sorted(d.keys()):
            v = d[k]
            new_key = self.flatten_key(k, parent_key, sep)
            if isinstance(v, collections.MutableMapping):
                items.extend(self.flatten_record(v, parent_key + [k], sep=sep).items())
            else:
                items.append((new_key, json.dumps(v) if type(v) is list else v))
        return dict(items)

    def append_process_date(self, records) -> dict:
        """A function that appends the current UTC to every record"""

        def process_date(record):
            record["_PROCESS_DATE"] = datetime.utcnow().isoformat()
            return record

        return list(map(lambda x: process_date(x), records))
