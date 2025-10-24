import os
from celery import Celery 
from dotenv import load_dotenv
load_dotenv()


REDIS_BROKER = os.getenv("REDIS_BROKER")
REDIS_BACKEND = os.getenv("REDIS_BACKEND")

celery = Celery(__name__, broker=REDIS_BROKER, backend=REDIS_BACKEND, include=['app.tasks'])

celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    enable_utc=True,
    task_protocol=1,   # important for gocelery compatibility
)

