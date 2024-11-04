import time

import requests
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed

# Rate limit: 5 requests per 60 seconds
CALLS = 10
PERIOD = 60

# Base URL for JSONPlaceholder
BASE_URL = "https://jsonplaceholder.typicode.com/posts"
PAGE_SIZE = 5  # Number of posts per page
TOTAL_POSTS = 100  # Total posts we want to fetch

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_page_with_retry_and_rate_limit(start):
    """
    Fetches a page of posts from JSONPlaceholder with retry and rate limiting.
    """
    params = {'_start': start, '_limit': PAGE_SIZE}
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

# Calculate the starting points for each page
page_starts = [i for i in range(0, TOTAL_POSTS, PAGE_SIZE)]

# List to store all the posts
all_posts = []

# ThreadPoolExecutor to fetch multiple pages concurrently
with ThreadPoolExecutor(max_workers=5) as executor:
    # Start fetching pages concurrently
    future_to_page = {executor.submit(fetch_page_with_retry_and_rate_limit, start): start for start in page_starts}

    for future in as_completed(future_to_page):
        t1 = time.time()
        start = future_to_page[future]
        try:
            page_data = future.result()
            all_posts.extend(page_data)
            t2 = time.time()
            print(f"Fetched posts starting from {start},  t1: {t1}, t2: {t2}, dt: {t2-t1}")
        except Exception as e:
            print(f"Failed to fetch posts starting from {start}: {e}")


print(f"Total posts fetched: {len(all_posts)}")
