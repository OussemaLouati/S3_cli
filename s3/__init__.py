"""Package for using S3."""
import os

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_URL = "s3://" + S3_BUCKET_NAME + "/"