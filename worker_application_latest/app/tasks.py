import subprocess
import os
import tempfile
import redis
import threading
import time
import re
import json
import queue
from mimetypes import guess_type
from app.s3_configuration import s3, STREAMING_BUCKET, PENDING_BUCKET
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
REDIS_URL = os.getenv("REDIS_URL")
redis_client_sync = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Regex to parse FFmpeg's time output: e.g., time=00:01:23.45
FFMPEG_TIME_REGEX = re.compile(r"time=(\d+):(\d+):(\d+\.\d+)")

def get_video_metadata(presigned_url):
    """Fetches duration and checks for audio in ONE single network trip."""
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_entries", "format=duration:stream=codec_type",
        presigned_url
    ]
    try:
        output = subprocess.check_output(cmd).decode('utf-8')
        data = json.loads(output)
        
        # Safely extract duration
        duration = float(data.get('format', {}).get('duration', 0.0))
        
        # Check if any stream is audio
        has_audio = any(stream.get('codec_type') == 'audio' for stream in data.get('streams', []))
                
        return duration, has_audio
    except Exception as e:
        print(f"Warning: Metadata probe failed: {e}")
        return 0.0, False

def process_video_from_s3(upload_id: str, s3_key: str):
    stream_name = "Event.Transcode.Status"
    job_status_stream = f"job_status:{upload_id}"

    with tempfile.TemporaryDirectory() as work_root:
        try:
            # 1. Generate presigned URL
            presigned_url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': PENDING_BUCKET, 'Key': s3_key},
                ExpiresIn=3600
            )
            print(f"Worker generated presigned URL for {s3_key}")

            # 2. Pre-flight probe for progress math and audio check
            total_duration, has_audio = get_video_metadata(presigned_url)
            print(f"Total video duration calculated: {total_duration}s. Audio present: {has_audio}")

            variants = [360, 480, 720, 1080]
            
            # Pre-create output directories for FFmpeg var_stream_map
            for res in variants:
                os.makedirs(os.path.join(work_root, f"{res}p"), exist_ok=True)

            # =========================================================================
            # GOD TIER OPTIMIZATION 2: The Concurrent Queue Uploader
            # =========================================================================
            upload_queue = queue.Queue(maxsize=2000)
            queued_files = set()
            stop_scanner = threading.Event()

            # consumer of queue
            def uploader_worker():
                """Consumes files from the queue and blasts them to S3 concurrently."""
                while True:
                    task = upload_queue.get()
                    if task is None:  # Sentinel value to kill the thread
                        break
                    
                    file_path, s3_dest, c_type, delete_after = task
                    try:
                        s3.upload_file(
                            file_path, STREAMING_BUCKET, s3_dest, 
                            ExtraArgs={"ContentType": c_type}
                        )
                        if delete_after:
                            os.remove(file_path) # Rolling Deletion!
                    except Exception as e:
                        print(f"Concurrent upload error for {os.path.basename(file_path)}: {e}")
                    finally:
                        upload_queue.task_done()

            # Spin up 10 lightweight upload threads
            uploader_threads = []
            for _ in range(10):
                t = threading.Thread(target=uploader_worker)
                t.start()
                uploader_threads.append(t)

            # producer 
            def scanner_logic():
                """Scans directories and pushes completely written .ts files to the queue."""
                while not stop_scanner.is_set():
                    for res in variants:
                        res_dir = os.path.join(work_root, f"{res}p")
                        for f in os.listdir(res_dir):
                            if f.endswith('.ts'):
                                file_path = os.path.join(res_dir, f)
                                if file_path not in queued_files:
                                    queued_files.add(file_path)
                                    s3_dest = f"videos/{upload_id}/{res}p/{f}"
                                    upload_queue.put((file_path, s3_dest, "video/MP2T", True))
                    time.sleep(1) # Poll every 1 second
                
                # Final sweep after FFmpeg finishes completely
                for res in variants:
                    res_dir = os.path.join(work_root, f"{res}p")
                    for f in os.listdir(res_dir):
                        if f.endswith('.ts') or f.endswith('.m3u8'):
                            file_path = os.path.join(res_dir, f)
                            if file_path not in queued_files:
                                queued_files.add(file_path)
                                s3_dest = f"videos/{upload_id}/{res}p/{f}"
                                c_type = "application/vnd.apple.mpegurl" if f.endswith('.m3u8') else "video/MP2T"
                                upload_queue.put((file_path, s3_dest, c_type, True))

            scanner_thread = threading.Thread(target=scanner_logic)
            scanner_thread.start()

            # =========================================================================
            # GOD TIER OPTIMIZATION 1: The Single-Download FFmpeg Muxer
            # =========================================================================
            # This filter splits the input 4 ways in RAM, then passes each to the GPU
            filter_complex = (
                "[0:v]split=4[v1][v2][v3][v4];"
                "[v1]hwupload_cuda,scale_cuda=w=-2:h=360:format=nv12:interp_algo=lanczos[o1];"
                "[v2]hwupload_cuda,scale_cuda=w=-2:h=480:format=nv12:interp_algo=lanczos[o2];"
                "[v3]hwupload_cuda,scale_cuda=w=-2:h=720:format=nv12:interp_algo=lanczos[o3];"
                "[v4]hwupload_cuda,scale_cuda=w=-2:h=1080:format=nv12:interp_algo=lanczos[o4]"
            )

            cmd = [
                "ffmpeg", "-y", "-threads", "0", 
                "-i", presigned_url,  
                "-filter_complex", filter_complex,
            ]

            # Dynamically add maps depending on audio presence
            if has_audio:
                cmd.extend([
                    "-map", "[o1]", "-map", "0:a",
                    "-map", "[o2]", "-map", "0:a",
                    "-map", "[o3]", "-map", "0:a",
                    "-map", "[o4]", "-map", "0:a",
                    "-c:a", "aac", "-b:a", "128k"
                ])
                var_stream_map = "v:0,a:0,name:360p v:1,a:1,name:480p v:2,a:2,name:720p v:3,a:3,name:1080p"
            else:
                cmd.extend([
                    "-map", "[o1]", 
                    "-map", "[o2]", 
                    "-map", "[o3]", 
                    "-map", "[o4]"
                ])
                var_stream_map = "v:0,name:360p v:1,name:480p v:2,name:720p v:3,name:1080p"

            # Universal video settings and HLS config
            cmd.extend([
                "-c:v", "h264_nvenc",
                "-preset", "p5", "-tune", "hq", "-rc", "vbr", "-cq", "24", "-b:v", "0",
                "-bf", "2", "-g", "48", "-keyint_min", "48",
                "-f", "hls",
                "-hls_time", "6",
                "-hls_playlist_type", "vod",
                "-hls_flags", "temp_file",
                "-hls_segment_filename", f"{work_root}/%v/seg_%03d.ts",
                "-var_stream_map", var_stream_map,
                f"{work_root}/%v/playlist.m3u8"
            ])

            print(f"Worker starting unified transcoding for {upload_id}")
            process = subprocess.Popen(cmd, stderr=subprocess.PIPE, universal_newlines=True)
            
            last_pushed_progress = 0.0
            last_ffmpeg_log = ""

            # 4. Stream Progress live to Redis
            for line in process.stderr:
                last_ffmpeg_log = line.strip()
                match = FFMPEG_TIME_REGEX.search(line)
                if match and total_duration > 0:
                    h, m, s = match.groups()
                    current_sec = int(h) * 3600 + int(m) * 60 + float(s)
                    
                    global_progress = (current_sec / total_duration) * 100
                    global_progress = min(global_progress, 99.0) # Cap at 99 until finished
                    
                    if global_progress - last_pushed_progress >= 2.0:
                        last_pushed_progress = global_progress
                        redis_client_sync.xadd(
                            job_status_stream,
                            {"status": "processing", "progress": str(int(global_progress))},
                            maxlen=10000, approximate=True, id="*"
                        )

            process.wait()
            if process.returncode != 0:
                raise RuntimeError(f"FFmpeg failed with error: {last_ffmpeg_log}")

            # 5. Gracefully shutdown the Watcher and Uploader Pool
            stop_scanner.set()
            scanner_thread.join()
            
            # Send sentinels to kill uploader threads and wait for queue to empty
            for _ in range(10):
                upload_queue.put(None)
            for t in uploader_threads:
                t.join()

            print(f"Worker finished all transcoding & uploads for {upload_id}")

            # 6. Generate & Upload Master Playlist
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

            s3.upload_file(
                Filename=master_playlist_path,
                Bucket=STREAMING_BUCKET,
                Key=f"videos/{upload_id}/master.m3u8",
                ExtraArgs={"ContentType": "application/vnd.apple.mpegurl"},
            )

            # 7. Push final success state to the main Go database stream
            redis_client_sync.xadd(
                stream_name,
                {"upload_id": upload_id, "status": "ready"},
                maxlen=10000, approximate=True, id="*"
            )
            
            return {"status": "success", "upload_id": upload_id}

        except Exception as e:
            print(f"Error processing {upload_id}: {e}")
            
            # Clean up the threads in case of mid-process failure
            stop_scanner.set()

            if 'scanner_thread' in locals() and scanner_thread.is_alive():
                scanner_thread.join()

            for _ in range(10):
                try: upload_queue.put_nowait(None)
                except: pass
            
            redis_client_sync.xadd(
                job_status_stream, {"status": "failed", "progress": "0"},
                maxlen=10000, approximate=True, id="*"
            )
            redis_client_sync.xadd(
                stream_name, {"upload_id": upload_id, "status": "failed"},
                maxlen=10000, approximate=True, id="*"
            )
            raise

        finally:
            # 8. CLEANUP
            try:
                s3.delete_object(Bucket=PENDING_BUCKET, Key=s3_key)
                print(f"Worker cleaned up s3://{PENDING_BUCKET}/{s3_key}")
            except Exception as e:
                print(f"Error cleaning up pending file for {upload_id}: {e}")