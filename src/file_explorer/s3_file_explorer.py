import io
from typing import Generator
from datetime import datetime, timezone
import boto3
from dotenv import load_dotenv
import os
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.paginator import ListObjectsV2Paginator

from src.models.file_info.file_info import FileInfo


class S3Explorer:
    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str,
        access_key_id: str,
        secret_access_key: str,
    ) -> None:
        self.bucket_name: str = bucket_name
        self._client: S3Client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    def upload_file(self, local_file_path: str, s3_path: str) -> None:
        """
        uploads local file to the specified s3 path
        """
        self._client.upload_file(local_file_path, self.bucket_name, s3_path)

    def download_to_buffer(self, s3_path: str) -> io.BytesIO:
        """
        downloads file from s3_path into a file buffer
        """
        buffer: io.BytesIO = io.BytesIO()
        self._client.download_fileobj(self.bucket_name, s3_path, buffer)
        buffer.seek(0)
        return buffer

    def list_files(
        self, s3_path_prefix: str, last_modified_date: datetime
    ) -> Generator[FileInfo, None, None]:
        """
        list all files under a given s3 path prefix and return a generator of file paths
        """

        paginator: ListObjectsV2Paginator = self._client.get_paginator(
            "list_objects_v2"
        )
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=s3_path_prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    # convert s3's timezone-aware modified date to utc first, then remove the timezone
                    converted_modified_date = (
                        obj["LastModified"].astimezone(timezone.utc).replace(tzinfo=None)
                    )
                    if converted_modified_date>last_modified_date:
                        yield FileInfo(
                            file_path=obj["Key"], modified_date=converted_modified_date
                        )


if __name__ == "__main__":
    load_dotenv()
    s3_explorer: S3Explorer = S3Explorer(
        bucket_name=os.getenv("AWS_S3_BUCKET", ""),
        endpoint_url=os.getenv("AWS_S3_ENDPOINT", ""),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )
    # test: upload sample eth_blocks CSV file to specified path
    s3_explorer.upload_file(
        "eth_blocks_20250106.csv",
        "chainstack/eth_blocks/2025/01/06/eth_blocks_20250106.csv",
    )
    files_to_read: Generator[FileInfo, None, None] = s3_explorer.list_files(
        "chainstack/eth_blocks", datetime(year=2025, month=1, day=7)
    )

    for file_info in files_to_read:
        # e.g s3_file_path = "chainstack/eth_blocks/2025/01/06/eth_blocks_20250106.csv"
        csv_file_bytes_io: io.BytesIO = s3_explorer.download_to_buffer(file_info.file_path)
        file_bytes = csv_file_bytes_io.read()
        print(file_bytes)
