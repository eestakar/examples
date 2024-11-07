import redis
import time
from functools import wraps

# Connect to Redis
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_PASSWORD = "your_strong_password"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=0)

# Constants for rate limits
MAX_CALLS_PER_SECOND = 100
MAX_SIMULTANEOUS_CALLS = 40
TIME_WINDOW = 1  # in seconds

def rate_limit(api_key):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Unique Redis keys
            count_key = f"api_count:{api_key}"
            queue_key = f"api_queue:{api_key}"

            # Ensure atomicity using Redis transaction pipeline
            with r.pipeline() as pipe:
                try:
                    # Start transaction
                    pipe.watch(count_key, queue_key)

                    # Get the current API call count and simultaneous calls
                    current_count = int(r.get(count_key) or 0)
                    current_queue = int(r.llen(queue_key))

                    # Check rate limits
                    if current_count >= MAX_CALLS_PER_SECOND:
                        # Wait until next second if we exceed 100 calls/sec
                        time.sleep(TIME_WINDOW)
                    elif current_queue >= MAX_SIMULTANEOUS_CALLS:
                        # Queue the request if max simultaneous calls reached
                        r.rpush(queue_key, 1)
                        # Block until our turn
                        while int(r.lindex(queue_key, 0)) != 1:
                            time.sleep(0.01)
                        r.lpop(queue_key)  # Remove from queue when ready

                    # Increment call count and set TTL
                    pipe.multi()
                    pipe.incr(count_key)
                    pipe.expire(count_key, TIME_WINDOW)  # Reset every second
                    pipe.execute()

                    # Execute the actual API call
                    return func(*args, **kwargs)

                finally:
                    # Clean up queue if call completes or fails
                    r.lrem(queue_key, 1, "1")

        return wrapper
    return decorator


@rate_limit(api_key="my_api_limit_key")
def my_api_view(request):
    # Your API logic here
    return {"status": "success"}
