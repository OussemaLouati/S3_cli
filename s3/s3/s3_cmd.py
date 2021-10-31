"""Module for interacting with s3 buckets."""
import argparse

import yaml

from s3 import S3_BUCKET_NAME
from s3.s3.s3_manager import S3Manager
from s3.util import log

logger = log.get_logger(__name__)


def parse_args():
    """Parse arguments."""
    parser = argparse.ArgumentParser(description="Interact with S3.")
    parser.add_argument("--bucket_name", type=str, default=S3_BUCKET_NAME, help="Get bucket name.")
    parser.add_argument("--list_all", action="store_true", help="List all files inside a bucket.")
    parser.add_argument("--find", action="store_true", help="Search for a file.")
    parser.add_argument("--info", action="store_true", help="Info about a file.")
    parser.add_argument("--delete", action="store_true", help="Delete files.")
    parser.add_argument("--upload", action="store_true", help="Upload files.")
    parser.add_argument("--download", action="store_true", help="Download files")
    parser.add_argument("--bucket_path", type=str, default=None, help="File Bucket Path")
    parser.add_argument("--local_path", type=str, default=None, help="File Local path")
    parser.add_argument("--prefix", type=str, default="", help="Prefix to filter out")
    parser.add_argument("--extra_info", action="store_true", help="Get extra info about a file")
    parser.add_argument("--file_name", type=str, default=None, help="Name of the file")

    args = parser.parse_args()

    return args


def main():  # noqa: CCR001
    """Interact with S3."""
    # parse arguments
    args = parse_args()

    manager = S3Manager()

    if args.list_all:
        files = manager.inspect_bucket(
            bucket=args.bucket_name, prefix=args.prefix, extra_info=args.extra_info
        )
        logger.info(yaml.dump(files))
    elif args.find:
        if args.file_name:
            files = manager.find_file(
                bucket=args.bucket_name, file_name=args.file_name, extra_info=args.extra_info
            )
            logger.info(yaml.dump(files))
        else:
            raise ValueError("You should pass a file name, eq: --file_name foo")
    elif args.upload:
        if not args.bucket_path:
            raise ValueError(
                "You should pass bucket path to save file under, eq: --bucket_path foo"
            )
        if not args.local_path:
            raise ValueError("You should pass the path to your local file, eq: --loca_path foo")
        path = manager.upload_file(
            file_name=args.local_path, bucket_folder=args.bucket_path, bucket=args.bucket_name
        )
        logger.info('File "%s" saved in the bucket under: %s.', args.local_path, path)

    elif args.download:
        if not args.bucket_path:
            raise ValueError("You should path of the file inside the bucket, eq: --bucket_path foo")
        if not args.local_path:
            raise ValueError(
                "You should pass path of a local dir to save the file under, eq: --local_path bar"
            )

        path = manager.download_file(
            bucket=args.bucket_name, local_folder=args.local_path, bucket_folder=args.bucket_path
        )
        logger.info('File "%s" saved in locally under: %s.', args.bucket_path, path)

    elif args.delete:
        if not args.bucket_path:
            raise ValueError(
                "You should pass the path of the file to delete, eq: --bucket_path foo"
            )

        deleted = manager.delete_file(bucket=args.bucket_name, file_name=args.bucket_path)
        if deleted:
            logger.info("%s is deleted.", args.bucket_path)
        else:
            logger.info("Please re-try, %s is not deleted.", args.bucket_path)

    elif args.info:
        if not args.bucket_path:
            raise ValueError(
                "You should pass path of the file you want to inspect, eq: --bucket_path foo"
            )

        file = manager.file_info(bucket=args.bucket_name, file_name=args.bucket_path)
        logger.info(file)

    else:
        raise ValueError(
            " You should choose between: --list_all, --find, --upload, --info, --delete, --download"
        )
