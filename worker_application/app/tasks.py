import subprocess
from mimetypes import guess_type
from concurrent.futures import ThreadPoolExecutor
from app.s3_configuration import s3, STREAMING_BUCKET, PENDING_BUCKET
from celery import shared_task
import os
import tempfile
import redis


redis_client_sync = redis.Redis(host='localhost', port=6379, db=0)



#new transcoding method
@shared_task(name='tasks.transcode_and_upload_video', queue='video_tasks', bind=True)
def process_video_from_s3(self, upload_id: str, s3_key: str):
    """
    Celery task that uses a presigned URL to stream a video directly
    from S3 into ffmpeg for transcoding.
    """

    channel = f"job_status_{upload_id}"

    with tempfile.TemporaryDirectory() as work_root:
        try:
            # 1. Genarating a presigned url for the private S3 object
            # This URL grants temporary access for ffmpeg to read the file.
            presigned_url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': PENDING_BUCKET, 'Key': s3_key},
                ExpiresIn=3600  # URL is valid for 1 hour
            )
            print(f"Worker generated presigned URL for {s3_key}")

            # 2. Transcode using the URL directly as input
            print(f"Worker starting transcoding for {upload_id} from URL")
            variants = [360, 480, 720, 1080]

            def transcode(res):
                out_dir = os.path.join(work_root, f"{res}p")
                os.makedirs(out_dir, exist_ok=True)
                playlist = os.path.join(out_dir, "playlist.m3u8")
                
                # The change here is: -i now uses the presigned URL
                cmd = [
                        "ffmpeg",
                        "-threads", "0",
                        "-i", presigned_url,
                        
                        # The explicit, high-quality GPU scaling filter
                        "-vf", f"hwupload_cuda,scale_cuda=w=-2:h={res}:format=nv12:interp_algo=lanczos",
                        
                        # Correct encoder settings for high-quality VOD
                        "-c:v", "h264_nvenc",
                        "-preset", "p5",          # Use a quality-focused preset (p5 is a great balance)
                        "-tune", "hq",           # Tune for High Quality (NOT low latency)
                        "-rc", "vbr",            # Use Variable Bitrate for efficiency
                        "-cq", "24",             # Constant Quality level
                        "-b:v", "0",             # Required for VBR/CQ mode
                        "-bf", "2",              # Allow 2 B-frames for better compression
                        "-g", "48",              # Shorter GOP size for better HLS performance
                        "-keyint_min", "48",
                        
                        # Audio and HLS settings (unchanged)
                        "-c:a", "aac", 
                        "-b:a", "128k",
                        "-f", "hls",
                        "-hls_time", "6",
                        "-hls_playlist_type", "vod",
                        "-hls_segment_filename", f"{out_dir}/seg_%03d.ts",
                        playlist
                ]
                subprocess.run(cmd, check=True)
                return out_dir

            with ThreadPoolExecutor() as exe:
                out_dirs = list(exe.map(transcode, variants))
            print(f"Worker finished transcoding for {upload_id}")
            
            # 3. Uploading the HLS folders 
            def upload_folder(local_dir, s3_prefix):
                for root, _, files in os.walk(local_dir):
                    for fname in files:
                        local_path = os.path.join(root, fname)
                        rel_path   = os.path.relpath(local_path, local_dir)
                        s3_key     = f"videos/{upload_id}/{s3_prefix}/{rel_path}"
                        content_type = guess_type(fname)[0] or "application/octet-stream"
                        s3.upload_file(
                            Filename=local_path,
                            Bucket=STREAMING_BUCKET,
                            Key=s3_key,
                            ExtraArgs={"ContentType": content_type},
                        )

            
            with ThreadPoolExecutor() as exe:
                for res, out_dir in zip(variants, out_dirs):
                    exe.submit(upload_folder, out_dir, f"{res}p")

            # master playlist generation..

            variant_info = {
                360: {"bandwidth": 800000, "resolution": "640x360"},
                480: {"bandwidth": 1400000, "resolution": "854x480"},
                720: {"bandwidth": 2800000, "resolution": "1280x720"},
                1080: {"bandwidth": 5000000, "resolution": "1920x1080"},
            }

            master_playlist_content = ['#EXTM3U', '#EXT-X-VERSION:3']
            for res in variants:
                info = variant_info[res]
                master_playlist_content.append(f'#EXT-X-STREAM-INF:BANDWIDTH={info["bandwidth"]},RESOLUTION={info["resolution"]}')
                master_playlist_content.append(f'{res}p/playlist.m3u8')
            
            master_playlist_path = os.path.join(work_root, "master.m3u8")
            with open(master_playlist_path, "w") as f:
                f.write("\n".join(master_playlist_content))

            # Uploading the master playlist
            s3.upload_file(
                Filename=master_playlist_path,
                Bucket=STREAMING_BUCKET,
                Key=f"videos/{upload_id}/master.m3u8",
                ExtraArgs={"ContentType": "application/vnd.apple.mpegurl"},
            )
            print(f"Worker uploaded master playlist for {upload_id} to s3://{STREAMING_BUCKET}/videos/{upload_id}/master.m3u8")

            redis_client_sync.publish(channel, "ready")
            
            return {"status": "success", "upload_id": upload_id}

        except Exception as e:
            print(f"Error processing {upload_id}: {e}")
            self.update_state(state='FAILURE', meta={'exc': str(e)})
            redis_client_sync.publish(channel, "failed")
            raise

        finally:
            # 4. CLEANUP 
            try:
                s3.delete_object(Bucket=PENDING_BUCKET, Key=s3_key)
                print(f"Worker cleaned up s3://{PENDING_BUCKET}/{s3_key}")
            except Exception as e:
                print(f"Error cleaning up pending file for {upload_id}: {e}")