import os
from dotenv import load_dotenv
import boto3
from botocore.client import Config

load_dotenv()

S3_ACCESS_USER_ID = str(os.getenv('MINIO_ROOT_USER'))
S3_SECRET_ACCESS_KEY = str(os.getenv('MINIO_ROOT_PASSWORD'))
S3_ENDPOINT = str(os.getenv('MINIO_ENDPOINT'))
STREAMING_BUCKET = "streaming"
PENDING_BUCKET = "pending"

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_USER_ID,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    config=Config(signature_version="s3v4")
)