# background_jobs.py

import os
import time
import threading
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Callable, Optional, Union

import redis
from redis import Redis


class BackgroundJobSystem:
    """A simple background job processing system using Redis."""

    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0, queue_name='default'):
        """Initialize the background job system with Redis connection."""
        self.redis_conn = Redis(host=redis_host, port=redis_port, db=redis_db)
        self.queue_name = queue_name
        self.workers: List[threading.Thread] = []
        self.stop_event = threading.Event()

    def enqueue_job(self,
                    func: Callable,
                    *args,
                    **kwargs) -> str:
        """
        Add a job to the queue to be processed in the background.
        Returns the job ID for tracking.
        """
        job_id = str(uuid.uuid4())

        # We can't store the function directly, so we store its module path and name
        func_path = f"{func.__module__}.{func.__name__}"

        job_data = {
            'id': job_id,
            'func_path': func_path,
            'args': args,
            'kwargs': kwargs,
            'status': 'queued',
            'created_at': datetime.now().isoformat(),
        }

        # Store job details in a Redis hash
        self.redis_conn.hset(f"job:{job_id}", mapping=job_data)

        # Add to queue
        self.redis_conn.lpush(f"queue:{self.queue_name}", job_id)

        print(f"Job {job_id} enqueued for {func_path}")
        return job_id

    def schedule_job(self,
                     func: Callable,
                     delay_seconds: int,
                     *args,
                     **kwargs) -> str:
        """Schedule a job to run after a specific delay."""
        job_id = str(uuid.uuid4())

        # Store function info
        func_path = f"{func.__module__}.{func.__name__}"

        # Calculate execution time
        execute_at = datetime.now() + timedelta(seconds=delay_seconds)
        execute_at_timestamp = int(execute_at.timestamp())

        job_data = {
            'id': job_id,
            'func_path': func_path,
            'args': args,
            'kwargs': kwargs,
            'status': 'scheduled',
            'execute_at': execute_at.isoformat(),
            'created_at': datetime.now().isoformat(),
        }

        # Store job details
        self.redis_conn.hset(f"job:{job_id}", mapping=job_data)

        # Add to sorted set for scheduling (score is execution timestamp)
        self.redis_conn.zadd("scheduled_jobs", {job_id: execute_at_timestamp})

        print(f"Job {job_id} scheduled for {execute_at}")
        return job_id

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the current status and details of a job."""
        job_data = self.redis_conn.hgetall(f"job:{job_id}")

        if not job_data:
            return {'error': f'Job {job_id} not found'}

        # Convert bytes to strings
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in job_data.items()}

    def start_worker(self, num_workers: int = 1):
        """Start background worker threads to process jobs from the queue."""
        for i in range(num_workers):
            worker = threading.Thread(target=self._worker_loop)
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
            print(f"Started worker #{i + 1}")

    def _worker_loop(self):
        """Worker thread function that processes jobs from the queue."""
        while not self.stop_event.is_set():
            # First check for scheduled jobs that are due
            self._check_scheduled_jobs()

            # Process jobs from queue
            job_id = self.redis_conn.brpop(f"queue:{self.queue_name}", timeout=1)

            if job_id:
                job_id = job_id[1].decode('utf-8')
                self._process_job(job_id)

    def _check_scheduled_jobs(self):
        """Check for scheduled jobs that are ready to be executed."""
        now = datetime.now()
        now_timestamp = int(now.timestamp())

        # Get jobs that are due (score <= current timestamp)
        due_jobs = self.redis_conn.zrangebyscore("scheduled_jobs", 0, now_timestamp)

        for job_id_bytes in due_jobs:
            job_id = job_id_bytes.decode('utf-8')

            # Remove from scheduled set
            self.redis_conn.zrem("scheduled_jobs", job_id)

            # Add to immediate execution queue
            self.redis_conn.lpush(f"queue:{self.queue_name}", job_id)

            # Update status
            self.redis_conn.hset(f"job:{job_id}", "status", "queued")
            print(f"Scheduled job {job_id} is now ready and queued")

    def _process_job(self, job_id: str):
        """Process a specific job by its ID."""
        try:
            # Get job details
            job_data = self.redis_conn.hgetall(f"job:{job_id}")
            if not job_data:
                print(f"Error: Job {job_id} not found")
                return

            # Convert bytes to strings
            job_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in job_data.items()}

            # Update status to 'processing'
            self.redis_conn.hset(f"job:{job_id}", "status", "processing")
            self.redis_conn.hset(f"job:{job_id}", "started_at", datetime.now().isoformat())

            # Import the function dynamically
            module_path, func_name = job_data['func_path'].rsplit('.', 1)
            module = __import__(module_path, fromlist=[func_name])
            func = getattr(module, func_name)

            # Parse args and kwargs (stored as strings)
            import ast
            args = ast.literal_eval(job_data.get('args', '()'))
            kwargs = ast.literal_eval(job_data.get('kwargs', '{}'))

            # Execute the function
            result = func(*args, **kwargs)

            # Update job with success status and result
            self.redis_conn.hset(f"job:{job_id}", "status", "completed")
            self.redis_conn.hset(f"job:{job_id}", "completed_at", datetime.now().isoformat())
            self.redis_conn.hset(f"job:{job_id}", "result", str(result))
            print(f"Job {job_id} completed successfully")

        except Exception as e:
            # Update job with error status
            error_msg = f"Error: {type(e).__name__}: {str(e)}"
            self.redis_conn.hset(f"job:{job_id}", "status", "failed")
            self.redis_conn.hset(f"job:{job_id}", "error", error_msg)
            self.redis_conn.hset(f"job:{job_id}", "completed_at", datetime.now().isoformat())
            print(f"Job {job_id} failed: {error_msg}")

    def stop_workers(self):
        """Stop all worker threads gracefully."""
        self.stop_event.set()
        for worker in self.workers:
            worker.join(timeout=5.0)
        self.workers = []
        print("All workers stopped")


# Example usage
if __name__ == "__main__":
    # Import example tasks
    from tasks import process_image, send_notification

    # Initialize the background job system
    job_system = BackgroundJobSystem(redis_host='localhost', redis_port=6379)

    # Start some worker threads
    job_system.start_worker(num_workers=2)

    try:
        # Enqueue immediate jobs
        for i in range(5):
            job_id = job_system.enqueue_job(
                process_image,
                f"image_{i}.jpg",
                filters=["resize", "enhance"]
            )
            print(f"Enqueued job: {job_id}")

        # Schedule delayed jobs
        for i in range(3):
            delay = (i + 1) * 5  # 5, 10, 15 seconds
            job_id = job_system.schedule_job(
                send_notification,
                delay,
                f"user_{i}@example.com",
                message=f"Your delayed task #{i} is complete!"
            )
            print(f"Scheduled job: {job_id} for {delay} seconds from now")

        # Keep the main thread running to allow workers to process
        print("\nMonitoring jobs... Press Ctrl+C to exit.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
        job_system.stop_workers()
        print("Done!")