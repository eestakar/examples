import redis
import time
from functools import wraps

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Constants for rate limits
MAX_CALLS_PER_SECOND = 100
MAX_SIMULTANEOUS_CALLS = 40
TIME_WINDOW = 1  # seconds

def rate_limit(api_key):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Redis keys for call count and queue length
            count_key = f"api_count:{api_key}"
            queue_key = f"api_queue:{api_key}"

            # Continuously check and wait if limits are exceeded
            while int(r.get(count_key) or 0) >= MAX_CALLS_PER_SECOND:
                time.sleep(0.02)  # Wait if we exceed 100 calls/sec

            while int(r.get(queue_key) or 0) >= MAX_SIMULTANEOUS_CALLS:
                time.sleep(0.01)  # Wait if we exceed max simultaneous calls

            # Start time for tracking elapsed time
            start_time = time.time()

            # Increment the queue and count
            r.incr(queue_key)
            r.incr(count_key)
            
            # Execute the API call
            try:
                result = func(*args, **kwargs)
            finally:
                # Decrement the queue
                r.decr(queue_key)

                # Decrease count only if a second has passed, else wait
                elapsed_time = time.time() - start_time
                if elapsed_time >= TIME_WINDOW:
                    r.decr(count_key)
                else:
                    time.sleep(TIME_WINDOW - elapsed_time)
                    r.decr(count_key)

            return result
        return wrapper
    return decorator
