import pyarrow

from pyarrow.parquet import ParquetWriter

from aiobotocore.session import AioSession

import s3fs

from target_s3.formats.format_base import FormatBase


class FormatParquet(FormatBase):
    def __init__(self, config, context) -> None:
        super().__init__(config, context, "parquet")
        self.create_filesystem()

    def create_filesystem(
        self,
    ) -> None:
        try:
            """Creates a s3fs FileSystem object for accessing S3."""
            # TODO: Consider using s3_client.upload_fileobj over S3FileSystem
            # We already defined session in FormatBase
            if self.aws_access_key and self.aws_secret_access_key:
                client_kwargs = dict(endpoint_url=self.endpoint_url)

                self.file_system = s3fs.S3FileSystem(
                    key=self.aws_access_key,
                    secret=self.aws_secret_access_key,
                    token=self.aws_session_token,
                    client_kwargs=client_kwargs,
                )
            elif self.aws_profile:
                session = AioSession(profile=self.aws_profile)
                self.file_system = s3fs.S3FileSystem(session=session)
            else:
                session = AioSession()
                self.file_system = s3fs.S3FileSystem(session=session)

        except Exception as e:
            self.logger.error("Failed to create parquet file system.")
            self.logger.error(e)
            raise e

    def validate(self, field, value):
        if isinstance(value, dict) and not value:
            # pyarrow can't process empty struct
            return None

        return value

    def create_dataframe(self) -> pyarrow.Table:
        """Creates a pyarrow Table object from the record set."""
        try:
            fields = set()
            for d in self.records:
                fields = fields.union(d.keys())
            dataframe = pyarrow.table(
                {
                    f: [self.validate(f, row.get(f)) for row in self.records]
                    for f in fields
                }
            )
        except Exception as e:
            self.logger.error("Failed to create parquet dataframe.")
            self.logger.error(e)
            raise e

        return dataframe

    def _prepare_records(self):
        # use default behavior, no additional prep needed
        return super()._prepare_records()

    def _write(self, contents: str = None) -> None:
        df = self.create_dataframe()
        try:
            ParquetWriter(
                f"{self.bucket}/{self.fully_qualified_key}.{self.extension}",
                df.schema,
                compression="gzip",  # TODO: support multiple compression types, inc snappy
                filesystem=self.file_system,
            ).write_table(df)
        except Exception as e:
            self.logger.error("Failed to write parquet file to S3.")
            raise e

    def run(self) -> None:
        # use default behavior, no additional run steps needed
        return super().run(self.context["records"])
