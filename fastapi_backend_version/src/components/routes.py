from fastapi import APIRouter , HTTPException, File , Response, Request, Query, BackgroundTasks
from sse_starlette.sse import EventSourceResponse
from dotenv import load_dotenv
from src.components.s3_configuration import s3, STREAMING_BUCKET, PENDING_BUCKET
from src.components.celery_app import celery
from urllib.parse import unquote_plus
import uuid
import mimetypes
import os, time, hashlib, base64, re, json, asyncio

router = APIRouter()

load_dotenv()


WEBHOOK_SECRET_TOKEN = os.getenv('WEBHOOK_SECRET_TOKEN')
URI_SIGNATURE_SECRET = os.getenv('URI_SIGNATURE_SECRET')
TTL = 3600


async def verify_webhook_token(token: str = Query(...)):
    if token != WEBHOOK_SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid secret token")

@router.post("/generate-upload-url/{filename}")
async def generate_upload_url(filename: str, request : Request):
    """Generate presigned URL for direct upload to pending bucket"""
    try:
        # Generate unique video ID
        video_id = uuid.uuid4().hex
        extension = filename.split('.')[-1] if '.' in filename else 'mp4'
        s3_key = f"pending/{video_id}.{extension}"

        content_type = mimetypes.guess_type(filename)[0]
        if not content_type:
            content_type = 'application/octet-stream'
        
        s3_client = request.app.state.s3_client
        
        # Create presigned URL (valid for 1 hour)
        local_presigned_url = await s3_client.generate_presigned_url(
            "put_object",
            Params={
                'Bucket': PENDING_BUCKET,
                'Key': s3_key,
                'ContentType': content_type
            },
            ExpiresIn=3600
        )

        proto = request.headers.get('x-forwarded-proto', 'http')
        host = request.headers.get('host',"")

        print("host - is ",host)

        if not host:
            return {
                "presigned_url": local_presigned_url,
                "video_id": video_id,
                "s3_key": s3_key
            }


        public_presigned_url = local_presigned_url.replace(
            'http://localhost:9000', 
            f'{proto}://{host}'
        )
        
        return {
            "presigned_url": public_presigned_url,
            "video_id": video_id,
            "s3_key": s3_key
        }
    
    except Exception as e:
        raise HTTPException(500, f"Failed to generate upload URL: {str(e)}")




@router.get("/stream-status/{video_id}")
async def stream_status(video_id: str, request: Request):
    async def event_generator():
        # Listen to a specific Redis channel for this upload
        channel = f"job_status_{video_id}"
        redis_client = request.app.state.redis_client
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(channel)
        
        while True:
            # Check if the client has disconnected
            if await request.is_disconnected():
                await pubsub.close()
                break
                
            # Wait for a message from the Celery worker on the Redis channel
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=20)
            
            if message:
                status = message['data'].decode('utf-8')
                # Send the status to the client
                yield {"data": json.dumps({"data": {"status": status}})}
                if status in ['ready', 'failed']:
                    break # Stop sending events if the job is done or failed
    
    return EventSourceResponse(event_generator())





@router.post("/s3-webhook")
async def handle_s3_event(request: Request):
    """Handle MinIO/S3 event notifications"""
    try:
        json_data = await request.json()
        print(json_data)
        record = json_data.get("Records",[])[0]
        print(record)
        encoded_s3_key = record.get("s3",{}).get("object",{}).get("key","")
        print(encoded_s3_key)
        s3_key = unquote_plus(encoded_s3_key)
        print(s3_key)

        if(not s3_key):
            raise HTTPException(422, "Malformed S3 event data: missing key")

        upload_id =s3_key.split("/")[1].split(".")[0]
        # 3) Kick off background work using celery
        celery.send_task('tasks.transcode_and_upload_video',args=[upload_id,s3_key],queue='video_tasks')

        return Response(status_code=200)

    except (IndexError, KeyError) as e:
        # This might happen if the event has an unexpected format
        raise HTTPException(422, f"Malformed S3 event data: {str(e)}")

    except Exception as e:
        raise HTTPException(500, f"Failed to handle S3 event: {str(e)}")



async def update_cache(redis_client, key: str, data: dict, ttl: int):
    """A simple function to be run in the background."""
    try:
        await redis_client.set(key, json.dumps(data), ex=ttl)
        print(f"BACKGROUND: Cached new playlist for {key}")
    except Exception as e:
        print(f"BACKGROUND: Redis cache write failed: {e}")

# signature plalist endpoint 
def md5hex(s: str) -> str:
    md5_bin = hashlib.md5(s).digest()
    return base64.urlsafe_b64encode(md5_bin).rstrip(b"=").decode("ascii")

def sign_uri(path_uri: str, expires_ts: int) -> str:
    # signature computed from: <st> + <uri> + <SECRET>
    raw = f"{expires_ts}{path_uri}{URI_SIGNATURE_SECRET}".encode("utf-8")
    return md5hex(raw)

def rewrite_playlist_blocking(playlist_content: str,
                              video_id: str,
                              resolution_path: str,
                              expires: int) -> str:
    # This regex finds any line that does NOT start with '#' or whitespace,
    # and captures the entire line. re.MULTILINE makes '^' match the start of each line.
    pattern = re.compile(r"^([^#\s].*)$", re.MULTILINE)
    def replacer(match):
        segment_file = match.group(1)
        public_path = f"/videos/{video_id}/{resolution_path}/{segment_file}"
        sig = sign_uri(public_path, expires)
        return f"{public_path}?st={expires}&sig={sig}"
    
    return pattern.sub(replacer,playlist_content)  # Use re.sub to perform the replacement in a single, fast operation

@router.get("/playlist/{video_id}/{resolution_path}")
async def signed_segments(video_id: str, resolution_path: str, request: Request, background_tasks: BackgroundTasks):

    s3_client = request.app.state.s3_client
    redis_client = request.app.state.redis_client

    REFRESH_THRESHOLD_SECONDS = 25 * 60  
    CACHE_TTL_SECONDS = TTL + 300

    cache_key = f"playlist:{video_id}:{resolution_path}"

    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            data = json.loads(cached_data)
            expire_time = data.get("expires_at")
            remaining_time = expire_time - int(time.time())

            if remaining_time > REFRESH_THRESHOLD_SECONDS:
                print(f"Cache HIT for {cache_key} (signatures still valid for {remaining_time}s)")
                return Response(content=data["playlist"], media_type="application/vnd.apple.mpegurl")
            else:
                print(f"Cache HIT for {cache_key} but signatures are stale (TTL: {remaining_time}s). Regenerating.")
    except Exception as e:
        print("cache read failed - ",str(e)) 

    print(f"Cache MISS for {cache_key}. Fetching from S3.")
    # fetch original master playlist from MinIO
    try:
        s3_key = f"videos/{video_id}/{resolution_path}/playlist.m3u8"
        response = await s3_client.get_object(Bucket=STREAMING_BUCKET, Key=s3_key)
        playlist_bytes = await response['Body'].read()
        playlist_content = playlist_bytes.decode('utf-8')
    except Exception as e:
        raise HTTPException(404, f"Failed to fetch master playlist: {str(e)}")

    # original = playlist_content.splitlines()
    expires = int(time.time()) + TTL
    
    loop = asyncio.get_running_loop()

    rewritten_playlist = await loop.run_in_executor(
        request.app.state.proc_pool,
        rewrite_playlist_blocking,
        playlist_content,
        video_id,
        resolution_path,
        expires
    )


    data_to_cache = {
             "playlist": rewritten_playlist,
             "expires_at": expires
    }

    background_tasks.add_task(
        update_cache, 
        redis_client, 
        cache_key, 
        data_to_cache, 
        CACHE_TTL_SECONDS
    )

    
    return Response(content=rewritten_playlist, media_type="application/vnd.apple.mpegurl")




@router.get("/master/{video_id}")
async def modified_master(video_id: str, request : Request, background_tasks: BackgroundTasks):

    s3_client = request.app.state.s3_client
    redis_client = request.app.state.redis_client

    cache_key = f"master:{video_id}"

    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            print(f"Cache HIT for {cache_key}")
            data = json.loads(cached_data)
            return Response(content=data["playlist"], media_type="application/vnd.apple.mpegurl")

    except:
        print("cache read failed")

    print("cache_miss_happened !!!")

    # fetch original master playlist from MinIO
    try:
        s3_key = f"videos/{video_id}/master.m3u8"
        response = await s3_client.get_object(Bucket=STREAMING_BUCKET, Key=s3_key)
        playlist_bytes = await response['Body'].read()
        playlist_content = playlist_bytes.decode('utf-8')
    except Exception as e:
        raise HTTPException(404, f"Failed to fetch master playlist: {str(e)}")

    original = playlist_content.splitlines()

    def make_modified_line(line: str) -> str:
        # if line is a comment or empty, return unchanged
        if line.strip() == "" or line.startswith("#"):
            return line
        
        # lines are like eg: "360p/playlist.m3u8"
        
        parts = line.split('/', 1)
        resolution_dir = parts[0]

        playlist_path = f"/api/playlist/{video_id}/{resolution_dir.lstrip('/').rstrip('/')}"

        return f"{playlist_path}"

    rewritten_lines = [make_modified_line(line) for line in original]
    rewritten_playlist = "\n".join(rewritten_lines) + "\n"

    data_to_cache = {
        "playlist": rewritten_playlist,
    }

    background_tasks.add_task(
        update_cache, 
        redis_client, 
        cache_key, 
        data_to_cache, 
        TTL
    )


    return Response(content=rewritten_playlist, media_type="application/vnd.apple.mpegurl")

@router.get("/status/{upload_id}")
async def get_status(upload_id: str, request: Request, background_tasks: BackgroundTasks):

    s3_client = request.app.state.s3_client
    redis_client = request.app.state.redis_client
    cache_key = f"upload_status:{upload_id}"

    try:
        cached_status = await redis_client.get(cache_key)
        if cached_status:
            print(f"Cache HIT for {upload_id}")
            # The data is stored as a JSON string, so we parse it back
            return json.loads(cached_status)
    except Exception as e:
        print(f"Cache MISS for {upload_id}: {str(e)}")

    prefix = f"videos/{upload_id}/"
    resp = await s3_client.list_objects_v2(Bucket=STREAMING_BUCKET, Prefix=prefix)
    contents = resp.get("Contents", [])

    if not contents:
        # No files at all yet
        raise HTTPException(404, "Upload ID not found")

    # Gather all keys under this upload
    keys = {obj["Key"].removeprefix(prefix) for obj in contents}

    status = "processing"
    resolutions = []

    # Check for master playlist
    if "master.m3u8" in keys:
        status = "ready"
        # Identify which variant folders are there
        for k in keys:
            # e.g. "360p/playlist.m3u8"
            if k.endswith("playlist.m3u8"):
                res_str = k.split("/")[0]  # "360p"
                if res_str.endswith("p"):
                    resolutions.append(int(res_str[:-1]))

    final_response = {
        "upload_id": upload_id,
        "status": status,
        "available_resolutions": sorted(resolutions)
    }

    background_tasks.add_task(
        update_cache, 
        redis_client, 
        cache_key, 
        final_response, 
        TTL
    )

    return final_response
