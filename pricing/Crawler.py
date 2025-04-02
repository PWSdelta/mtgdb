# Import required libraries
import requests
import logging
import time
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import concurrent.futures
import re
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sitemap_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SitemapCrawler:
    def __init__(self, base_url, max_workers=5, max_retries=3):
        """
        Initialize the sitemap crawler.

        Args:
            base_url (str): The base URL of the site
            max_workers (int): Maximum number of concurrent workers
            max_retries (int): Maximum number of retry attempts for failed requests
        """
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.card_urls = set()
        self.product_urls = set()
        self.all_urls = set()

        # Define headers to mimic a browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }

    def find_sitemaps(self):
        """
        Find all sitemaps for the website.

        Returns:
            list: List of sitemap URLs
        """
        sitemap_urls = []

        # First try robots.txt
        try:
            robots_url = f"{self.base_url}/robots.txt"
            logger.info(f"Checking {robots_url} for sitemaps")
            response = requests.get(robots_url, headers=self.headers, timeout=30)

            if response.status_code == 200:
                # Look for Sitemap: entries in robots.txt
                for line in response.text.splitlines():
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        sitemap_urls.append(sitemap_url)
                        logger.info(f"Found sitemap in robots.txt: {sitemap_url}")
        except Exception as e:
            logger.warning(f"Error checking robots.txt: {str(e)}")

        # If no sitemaps found in robots.txt, check common locations
        if not sitemap_urls:
            common_sitemap_paths = [
                '/sitemap.xml',
                '/sitemap_index.xml',
                '/sitemap-index.xml',
                '/sitemapindex.xml',
                '/sitemap/sitemap.xml'
            ]

            for path in common_sitemap_paths:
                try:
                    sitemap_url = self.base_url + path
                    logger.info(f"Checking common sitemap location: {sitemap_url}")
                    response = requests.head(sitemap_url, headers=self.headers, timeout=10)

                    if response.status_code == 200:
                        sitemap_urls.append(sitemap_url)
                        logger.info(f"Found sitemap at: {sitemap_url}")
                except Exception as e:
                    logger.debug(f"Error checking {path}: {str(e)}")

        return sitemap_urls

    def parse_sitemap(self, sitemap_url):
        """
        Parse a sitemap and extract URLs.

        Args:
            sitemap_url (str): URL of the sitemap

        Returns:
            list: List of URLs found in the sitemap
        """
        urls = []
        try:
            logger.info(f"Parsing sitemap: {sitemap_url}")
            response = requests.get(sitemap_url, headers=self.headers, timeout=30)

            if response.status_code != 200:
                logger.warning(f"Failed to fetch sitemap {sitemap_url}: HTTP {response.status_code}")
                return urls

            # Parse XML
            root = ET.fromstring(response.content)

            # Handle sitemap index (collection of sitemaps)
            # Look for <sitemap> tags in default or custom namespace
            namespace = root.tag.split('}')[0] + '}' if '}' in root.tag else ''
            sitemap_tag = f"{namespace}sitemap" if namespace else "sitemap"
            loc_tag = f"{namespace}loc" if namespace else "loc"

            sitemaps = root.findall(f".//{sitemap_tag}")
            if sitemaps:
                logger.info(f"Found sitemap index with {len(sitemaps)} sitemaps")
                for sitemap in sitemaps:
                    loc = sitemap.find(f".//{loc_tag}")
                    if loc is not None and loc.text:
                        sub_sitemap_url = loc.text.strip()
                        # Recursively parse sub-sitemaps
                        urls.extend(self.parse_sitemap(sub_sitemap_url))
            else:
                # Handle regular sitemap (collection of URLs)
                # Look for <url> tags
                url_tag = f"{namespace}url" if namespace else "url"
                for url_element in root.findall(f".//{url_tag}"):
                    loc = url_element.find(f".//{loc_tag}")
                    if loc is not None and loc.text:
                        page_url = loc.text.strip()
                        urls.append(page_url)

                logger.info(f"Found {len(urls)} URLs in sitemap {sitemap_url}")

        except ET.ParseError as e:
            logger.warning(f"XML parsing error in {sitemap_url}: {str(e)}")
            # Try to handle it as a text file (some sitemaps are just lists of URLs)
            try:
                response = requests.get(sitemap_url, headers=self.headers, timeout=30)
                for line in response.text.splitlines():
                    line = line.strip()
                    if line.startswith('http'):
                        urls.append(line)
                logger.info(f"Parsed {len(urls)} URLs from text sitemap {sitemap_url}")
            except Exception as e2:
                logger.error(f"Failed to parse {sitemap_url} as text: {str(e2)}")

        except Exception as e:
            logger.error(f"Error parsing sitemap {sitemap_url}: {str(e)}")

        return urls

    def filter_urls(self, urls):
        """
        Filter URLs to find card and product pages.

        Args:
            urls (list): List of URLs to filter

        Returns:
            tuple: (card_urls, product_urls, other_urls)
        """
        card_urls = set()
        product_urls = set()
        other_urls = set()

        for url in urls:
            path = urlparse(url).path.lower()

            if '/card/' in path:
                card_urls.add(url)
            elif '/product/' in path:
                product_urls.add(url)
            else:
                other_urls.add(url)

        logger.info(
            f"Filtered URLs: {len(card_urls)} card pages, {len(product_urls)} product pages, {len(other_urls)} other pages")
        return card_urls, product_urls, other_urls

    def crawl_url(self, url):
        """
        Crawl a single URL and extract information.
        Currently just visits the URL to verify it exists.

        Args:
            url (str): URL to crawl

        Returns:
            bool: Success or failure
        """
        retries = 0
        while retries <= self.max_retries:
            try:
                logger.info(f"Visiting {url}")
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()

                # Here you could extract more information from the page if needed
                # For example, get the title:
                soup = BeautifulSoup(response.text, 'html.parser')
                title = soup.title.string if soup.title else 'No title'

                logger.info(f"Successfully visited {url} - {title}")
                return True

            except RequestException as e:
                retries += 1
                if retries <= self.max_retries:
                    delay = 1 * (2 ** retries)
                    logger.warning(
                        f"Error visiting {url}: {str(e)}. Retrying in {delay} seconds. (Attempt {retries}/{self.max_retries})")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to visit {url} after {self.max_retries} retries: {str(e)}")

            except Exception as e:
                logger.error(f"Unexpected error visiting {url}: {str(e)}")
                break

        return False

    def run(self):
        """
        Run the sitemap crawler.

        Returns:
            tuple: (card_urls, product_urls, all_urls)
        """
        start_time = time.time()

        # Find sitemaps
        sitemap_urls = self.find_sitemaps()

        if not sitemap_urls:
            logger.warning("No sitemaps found. Trying default sitemap location.")
            sitemap_urls = [f"{self.base_url}/sitemap.xml"]

        # Parse all sitemaps and collect URLs
        all_urls = []
        for sitemap_url in sitemap_urls:
            urls = self.parse_sitemap(sitemap_url)
            all_urls.extend(urls)

        # Remove duplicates
        all_urls = list(set(all_urls))
        logger.info(f"Found {len(all_urls)} unique URLs in all sitemaps")

        # Filter URLs to find card and product pages
        card_urls, product_urls, other_urls = self.filter_urls(all_urls)

        # Save the URLs to files
        with open('card_urls.txt', 'w') as f:
            for url in sorted(card_urls):
                f.write(f"{url}\n")

        with open('product_urls.txt', 'w') as f:
            for url in sorted(product_urls):
                f.write(f"{url}\n")

        logger.info(f"Saved {len(card_urls)} card URLs to card_urls.txt")
        logger.info(f"Saved {len(product_urls)} product URLs to product_urls.txt")

        # Optional: Verify that the URLs actually exist by visiting them
        logger.info("Verifying card and product URLs...")
        urls_to_verify = list(card_urls) + list(product_urls)

        valid_urls = set()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.crawl_url, url): url for url in urls_to_verify}

            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    success = future.result()
                    if success:
                        valid_urls.add(url)
                except Exception as e:
                    logger.error(f"Error processing {url}: {str(e)}")

        # Save only valid URLs to separate files
        with open('valid_urls.txt', 'w') as f:
            for url in sorted(valid_urls):
                f.write(f"{url}\n")

        elapsed_time = time.time() - start_time
        logger.info(f"Sitemap crawl completed in {elapsed_time:.2f} seconds")
        logger.info(f"Found {len(card_urls)} card URLs, {len(product_urls)} product URLs")
        logger.info(f"Verified {len(valid_urls)} valid URLs out of {len(urls_to_verify)}")

        return card_urls, product_urls, all_urls


# Example usage
if __name__ == "__main__":
    # Replace with your website URL
    BASE_URL = "https://pwsdelta.com"

    crawler = SitemapCrawler(
        base_url=BASE_URL,
        max_workers=10,
        max_retries=3
    )

    # Run the crawler
    card_urls, product_urls, all_urls = crawler.run()