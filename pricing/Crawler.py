import requests
from bs4 import BeautifulSoup
import threading
import queue
import time
import urllib.parse
import re
from collections import defaultdict
import random


class URLQueue:
    """
    A queue system for managing URLs to crawl with duplicate detection
    """

    def __init__(self, delay_range=(0.5, 1)):
        self.queue = queue.Queue()
        self.visited = set()
        self.lock = threading.Lock()
        self.active = True
        self.delay_range = delay_range
        self.stats = defaultdict(int)

    def add_url(self, url):
        """Add a URL to the queue if it hasn't been visited"""
        with self.lock:
            if url not in self.visited:
                self.queue.put(url)
                self.visited.add(url)
                self.stats['queued'] += 1

                return True
        return False

    def get_url(self):
        """Get a URL from the queue with a random delay"""
        try:
            url = self.queue.get(block=True, timeout=0.11)
            # Add a small random delay
            # delay = random.uniform(*self.delay_range)
            self.stats['processed'] += 1
            return url
        except queue.Empty:
            return None

    def task_done(self):
        """Mark a task as done"""
        self.queue.task_done()

    def is_empty(self):
        """Check if the queue is empty"""
        return self.queue.empty()

    def shutdown(self):
        """Shutdown the queue"""
        self.active = False

    def get_stats(self):
        """Get current statistics"""
        with self.lock:
            return dict(self.stats)


class WebCrawler:
    """
    Concurrent web crawler that stays within specific domain and URL patterns
    """

    def __init__(self, base_url, allowed_patterns=None, num_workers=12):
        self.base_url = base_url
        self.domain = urllib.parse.urlparse(base_url).netloc
        self.url_queue = URLQueue()
        self.allowed_patterns = allowed_patterns or []
        self.num_workers = num_workers
        self.workers = []

    def is_valid_url(self, url):
        """Check if a URL is valid for crawling"""
        parsed = urllib.parse.urlparse(url)

        # Stay on the same domain
        if parsed.netloc != self.domain:
            return False

        # Check if URL matches any of the allowed patterns
        if self.allowed_patterns:
            path = parsed.path
            return any(re.search(pattern, path) for pattern in self.allowed_patterns)

        return True

    def extract_links(self, page_content, current_url):
        """Extract all links from page content"""
        soup = BeautifulSoup(page_content, 'html.parser')
        base_url_parts = urllib.parse.urlparse(current_url)
        links = []

        for anchor in soup.find_all('a', href=True):
            href = anchor['href']

            # Convert relative URLs to absolute
            if not href.startswith(('http://', 'https://')):
                if href.startswith('/'):
                    href = f"{base_url_parts.scheme}://{base_url_parts.netloc}{href}"
                else:
                    path = '/'.join(base_url_parts.path.split('/')[:-1]) + '/'
                    href = f"{base_url_parts.scheme}://{base_url_parts.netloc}{path}{href}"

            if self.is_valid_url(href):
                links.append(href)

        return links

    def worker(self):
        """Worker thread function"""
        while self.url_queue.active:
            url = self.url_queue.get_url()
            if url is None:
                # Check if all workers are waiting and queue is empty
                if self.url_queue.is_empty():
                    break
                continue

            try:
                self.process_url(url)
            except Exception as e:
                print(f"Error processing {url}: {e}")
            finally:
                self.url_queue.task_done()

    def process_url(self, url):
        """Process a single URL"""
        print(f"Crawling: {url}")

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                self.url_queue.stats['success'] += 1

                # Extract all links and add valid ones to the queue
                links = self.extract_links(response.text, url)
                for link in links:
                    self.url_queue.add_url(link)
            else:
                self.url_queue.stats['failed'] += 1
                print(f"Failed to fetch {url}, status code: {response.status_code}")
        except Exception as e:
            self.url_queue.stats['error'] += 1
            print(f"Exception while fetching {url}: {e}")

    def start(self):
        """Start the crawler with the base URL"""
        # Add the starting URL
        self.url_queue.add_url(self.base_url)

        # Create and start worker threads
        for _ in range(self.num_workers):
            thread = threading.Thread(target=self.worker)
            thread.daemon = True
            self.workers.append(thread)
            thread.start()

        # Wait for the queue to be empty
        try:
            # Print progress every 11 seconds
            while any(thread.is_alive() for thread in self.workers):
                stats = self.url_queue.get_stats()
                print(f"Progress: {stats}")
                time.sleep(11)
        except KeyboardInterrupt:
            print("Crawler interrupted by user")
            self.url_queue.shutdown()

        # Print final statistics
        print(f"Crawl completed. Final statistics: {self.url_queue.get_stats()}")


# Example usage script
if __name__ == "__main__":
    # Define your base URL and allowed patterns
    BASE_URL = "https://pwsdelta.com"
    ALLOWED_PATTERNS = [
        r"^/card/",
        r"^/cards/",
        r"^/product/"
    ]

    # Create and start the crawler
    crawler = WebCrawler(
        base_url=BASE_URL,
        allowed_patterns=ALLOWED_PATTERNS,
        num_workers=6
    )

    print(f"Starting crawler at {BASE_URL} with {crawler.num_workers} workers")
    print(f"Allowed URL patterns: {ALLOWED_PATTERNS}")

    # Start the crawl
    crawler.start()