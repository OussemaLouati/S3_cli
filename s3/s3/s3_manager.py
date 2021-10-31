"""Module defines basic upload/download operationss on an s3 bucket ."""
import logging
import os
from typing import List

import boto3
from botocore.exceptions import ClientError

from s3 import S3_BUCKET_NAME, S3_URL
from s3.s3.progress_bar_callback import ProgressPercentage
from s3.util import log

logger = log.get_logger(__name__)


class S3Manager:
    """An S3 utility manager that uploads/downloads files to s3."""

    def __init__(self):
        """Init."""
        aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        endpoint = os.getenv("S3_ENDPOINT", "")
        endpoint_url = "https://" + endpoint

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
        )

    def upload_file(
        self, file_name: str, bucket: str = S3_BUCKET_NAME, bucket_folder: str = None
    ) -> str:
        """Upload a file to an S3 bucket.

        Args:
            file_name: File to upload.
            bucket: Bucket to upload to.
            bucket_folder: path inside the bucket to upload the file to.

        Returns:
            Path of the file inside the bucket.
        """
        file_name_ = (file_name + ".")[:-1]
        if "/" in file_name:
            file_name = file_name.split("/")[-1]

        object_name = (
            file_name_
            if bucket_folder is None
            else bucket_folder.split(S3_URL)[1] + "/" + file_name
        )

        try:
            self.s3_client.upload_file(
                file_name_,
                bucket,
                object_name,
            )
            logger.info("File %s has been uploaded to s3 bucket %s", file_name_, bucket)
        except ClientError as e:
            logger.error(
                "File %s was not uploaded to s3 bucket %s with error %s",
                file_name_,
                bucket,
                e,
            )
        return object_name

    def upload_files(
        self, file_names: List[str], bucket: str = S3_BUCKET_NAME, bucket_folder: str = None
    ) -> List[str]:
        """Upload multiple files to an S3 bucket.

        Args:
            file_names: Files to upload.
            bucket: Bucket to upload to.
            bucket_folder: path inside the bucket to upload the file to.

        Returns:
            Paths of the files inside the bucket.
        """
        paths = []
        for file_name in file_names:
            path = self.upload_file(file_name=file_name, bucket=bucket, bucket_folder=bucket_folder)
            paths.append(path)
        return paths

    def download_file(
        self, bucket_folder: str, local_folder: str, bucket: str = S3_BUCKET_NAME
    ) -> str:
        """Download a file from an S3 bucket.

        Args:
            local_folder: Path to local folder to download file to.
            bucket: Bucket to use.
            bucket_folder: path inside the bucket to download the file.

        Returns:
            Path where file is saved locally.
        """
        bucket_folder = bucket_folder.split(S3_URL)[1]
        file_name = local_folder + "/" + bucket_folder.split("/")[-1]
        progress = ProgressPercentage(self.s3_client, bucket, bucket_folder)
        try:
            self.s3_client.download_file(bucket, bucket_folder, file_name, Callback=progress)
            logger.info("File %s has been downloaded from s3 bucket %s", file_name, bucket)
        except ClientError as e:
            logging.error(e)
        return file_name

    def delete_file(self, bucket: str = S3_BUCKET_NAME, file_name: str = None) -> bool:
        """Download a file from an S3 bucket.

        Args:
            bucket: Bucket to use.
            file_name: Full path of the file inside the bucket .

        Returns:
            Returns True if file deleted.
        """
        try:
            self.s3_client.delete_object(
                Bucket=bucket,
                Key=file_name,
            )
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def inspect_bucket(
        self,
        bucket: str = S3_BUCKET_NAME,
        prefix: str = "",
        extra_info: bool = False,
        max_keys: int = 100000000,
        start_after: str = "",
    ) -> list:
        """Inspect bucket.

        Args:
            bucket: Bucket to use.
            prefix: Filter the output to files that begin with a certain prefix.
            extra_info: Return extra info about the files
            max_keys: max number of returned files
            start_after: StartAfter is where you want Amazon S3 to start listing from.

        Returns:
            Returns True if file deleted.
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, MaxKeys=max_keys, StartAfter=start_after
            )
        except ClientError as e:
            logging.error(e)
        if "Contents" in response.keys():
            result = (
                response["Contents"] if extra_info else [obj["Key"] for obj in response["Contents"]]
            )
            return result
        return []

    def file_info(self, bucket: str = S3_BUCKET_NAME, file_name: str = None) -> dict:
        """Get info about a  file.

        Args:
            bucket: Bucket to use.
            file_name: Full path of the file inside the bucket .

        Returns:
            Returns info about a file.
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=file_name)
            return response
        except ClientError as e:
            logging.error(e)
        return {}

    def find_file(
        self,
        bucket: str = S3_BUCKET_NAME,
        file_name: str = None,
        extra_info: bool = False,
    ) -> list:
        """Get info about a  file.

        Args:
            bucket: Bucket to use.
            file_name: Full path of the file inside the bucket.
            extra_info: Show more details about file such as size, last modified...

        Returns:
            Returns all files that are similar to filename.
        """
        files = self.inspect_bucket(bucket=bucket, extra_info=extra_info)
        result = []
        for file in files:
            if file_name in file:
                result.append(file)
            elif extra_info and (file_name in file["Key"]):
                result.append(file)
        return result
