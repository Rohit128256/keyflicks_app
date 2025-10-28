import uvicorn
import json
from fastapi import FastAPI
from src.components.s3_configuration import s3, STREAMING_BUCKET, PENDING_BUCKET, lifespan
from src.components.routes import router


app = FastAPI(debug=True,lifespan=lifespan)

@app.on_event("startup")
def ensure_bucket():
    # 1. Create the bucket if it doesn't exist
    for bucket_name in [STREAMING_BUCKET, PENDING_BUCKET]:
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists.")
        except s3.exceptions.ClientError:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created.")

    # 2. Define and apply a public-read policy
    # policy = {
    #     "Version": "2012-10-17",
    #     "Statement": [
    #         {
    #             "Effect": "Allow",
    #             "Principal": "*",
    #             "Action": ["s3:GetObject"],
    #             "Resource": [f"arn:aws:s3:::{STREAMING_BUCKET}/*"]
    #         }
    #     ]
    # }
    # s3.put_bucket_policy(Bucket=STREAMING_BUCKET, Policy=json.dumps(policy))
    # print(f"Public-read policy applied to bucket '{STREAMING_BUCKET}'.")


app.include_router(router,prefix="/api")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)


