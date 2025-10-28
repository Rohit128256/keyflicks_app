import os
from dotenv import load_dotenv
from concurrent.futures import ProcessPoolExecutor
import boto3
import redis.asyncio as redis
from botocore.client import Config
from contextlib import asynccontextmanager
import aioboto3
from fastapi import FastAPI

load_dotenv()

S3_ACCESS_USER_ID = str(os.getenv('MINIO_ROOT_USER'))
S3_SECRET_ACCESS_KEY = str(os.getenv('MINIO_ROOT_PASSWORD'))
S3_ENDPOINT = str(os.getenv('MINIO_ENDPOINT'))
STREAMING_BUCKET = str(os.getenv('STREAMING_BUCKET'))
PENDING_BUCKET = str(os.getenv('PENDING_BUCKET'))

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_USER_ID,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    config=Config(signature_version="s3v4")
)

@asynccontextmanager
async def lifespan(app : FastAPI):
    session = aioboto3.Session()

    s3_context = session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_USER_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4")
    )

    s3_client = await s3_context.__aenter__()

    app.state.s3_session = session
    app.state.s3_client = s3_client

    pool_size = max(1, (os.cpu_count() or 1))
    app.state.proc_pool = ProcessPoolExecutor(max_workers=pool_size)

    redis_client = redis.Redis(host='localhost', port=6379)
    app.state.redis_client = redis_client
    try:
        yield
    finally:
        # Shutdown: close the client cleanly
        print("App shutting down... closing s3 client")
        await s3_context.__aexit__(None, None, None)
        proc_pool = getattr(app.state, "proc_pool", None)
        if proc_pool is not None:
            proc_pool.shutdown(wait=True)
        print("S3 client closed gracefully.")