import os
import time
import argparse
import multiprocessing
import redis
import sys
from app.tasks import process_video_from_s3
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
REDIS_URL = os.getenv("REDIS_URL")
STREAM_NAME = "video_processing_stream" # Must match what Go sends
GROUP_NAME = "python_video_workers"

def run_worker(worker_id):
    """The main loop for a single worker process"""
    consumer_name = f"worker_{worker_id}"
    print(f"[{consumer_name}] Starting up...")

    # 1. Connect to Upstash Redis
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True) # decode_responses=True automatically converts bytes to strings

    # 2. Ensure Consumer Group Exists
    try:
        r.xgroup_create(STREAM_NAME, GROUP_NAME, id='0', mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            print(f"[{consumer_name}] Redis error creating group: {e}")
            sys.exit(1)

    # 3. The Infinite Polling Loop
    while True:
        try:
            # Block 0 Upstash efficiency
            response = r.xreadgroup(
                GROUP_NAME, 
                consumer_name, 
                {STREAM_NAME: '>'}, 
                count=1, 
                block=0 
            )

            if not response:
                continue # time limit minutes passed, nothing happened. Loop restarts instantly.

            # 4. Parse the message
            for stream, messages in response:
                for msg_id, payload in messages:
                    print(f"[{consumer_name}] Received task: {msg_id}")
                    
                    try:
                        # --------------------------------------------------
                        # start the video processing
                        process_video_from_s3(payload['video_id'],payload['s3_key'])
                        # --------------------------------------------------
                        
                        # 5. Acknowledge the message upon SUCCESS
                        r.xack(STREAM_NAME, GROUP_NAME, msg_id)
                        print(f"[{consumer_name}] Successfully completed and ACKed: {msg_id}")

                    except Exception as task_err:
                        # If your task fails, it skips the XACK. 
                        # The message stays in the Pending queue to be retried!
                        print(f"[{consumer_name}] Task failed: {task_err}")

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            # Silently catch Upstash forcefully dropping the idle TCP connection
            time.sleep(0.1)
            continue
        except Exception as e:
            print(f"[{consumer_name}] Unexpected stream error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    # Allow passing --workers from the command line
    parser = argparse.ArgumentParser(description="Start Python Redis Workers")
    parser.add_argument("--workers", type=int, default=2, help="Number of concurrent worker processes")
    args = parser.parse_args()

    print(f"Starting {args.workers} background workers...")
    
    processes = []
    
    # Spawn the exact number of processes requested
    for i in range(args.workers):
        p = multiprocessing.Process(target=run_worker, args=(i+1,),daemon=True)
        p.start()
        processes.append(p)

    # Keep the main thread alive while workers run in the background
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[!] Forcefully shutting down all workers...")
        os._exit(0)