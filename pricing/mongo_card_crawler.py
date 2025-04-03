# concurrent_card_crawler.py
import requests
from sqlalchemy import create_engine, MetaData, Table, select
from bs4 import BeautifulSoup
import time
import random
import os
import concurrent.futures
import logging
from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm
import json
import backoff

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("card_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# PostgreSQL Database connection
pg_engine = create_engine(os.getenv('RW_DATABASE_URL', 'postgresql://postgres:asdfghjkl@localhost:5432/mtgdb'))
metadata = MetaData()
metadata.bind = pg_engine

# MongoDB connection
mongo_client = MongoClient(os.getenv('MONGO_URL', 'mongodb://localhost:27017/'))
mongo_db = mongo_client[os.getenv('MONGO_DB', 'card_crawler')]
crawl_results = mongo_db['crawl_results']
crawl_stats = mongo_db['crawl_stats']

# Reflect card_details table
card_details = Table('card_details', metadata, autoload_with=pg_engine)

# User agent list for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
]

# Base URL for the card pages
BASE_URL = "https://pwsdelta.com/card/{0}"

# Configuration
MAX_WORKERS = 8
MAX_RETRIES = 3
MIN_REQUEST_INTERVAL = 0.2  # seconds
MAX_REQUEST_INTERVAL = 0.5  # seconds
BATCH_SIZE = 100  # Process cards in batches to avoid memory issues

# Optional proxy configuration
# Set to None if not using proxies
PROXIES = None
# Example if using proxies:
# PROXIES = [
#     'http://user:pass@proxy1.example.com:8080',
#     'http://user:pass@proxy2.example.com:8080'
# ]

def get_card_ids(limit=None):
    """Get card IDs from the database with optional limit"""
    query = select(card_details.c.id)
    if limit:
        query = query.limit(limit)
    
    with pg_engine.connect() as connection:
        result = connection.execute(query)
        return [row[0] for row in result]

def get_headers():
    """Get random user agent for request headers"""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }

def get_proxy():
    """Get a random proxy if PROXIES is configured"""
    if PROXIES:
        return {'http': random.choice(PROXIES), 'https': random.choice(PROXIES)}
    return None

@backoff.on_exception(backoff.expo, 
                     (requests.exceptions.RequestException, 
                      requests.exceptions.HTTPError),
                     max_tries=MAX_RETRIES)
def make_request(url, card_id):
    """Make HTTP request with exponential backoff retry"""
    headers = get_headers()
    proxies = get_proxy()
    
    time.sleep(random.uniform(MIN_REQUEST_INTERVAL, MAX_REQUEST_INTERVAL))
    
    try:
        response = requests.get(
            url, 
            headers=headers, 
            proxies=proxies,
            timeout=30
        )
        response.raise_for_status()  # Raise exception for 4XX/5XX status codes
        return response
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:  # Too Many Requests
            logger.warning(f"Rate limited for card ID {card_id}. Backing off...")
            # Add additional delay for rate limiting
            time.sleep(5 + random.uniform(1, 5))
        raise e

def extract_card_data(soup, card_id):
    """Extract relevant data from the card page"""
    data = {
        'card_id': card_id,
        'crawl_timestamp': datetime.now(),
        'title': soup.find('title').text if soup.find('title') else None,
        # Add more data extraction logic based on your page structure
        # For example:
        # 'price': soup.find('span', class_='price').text.strip() if soup.find('span', class_='price') else None,
        # 'availability': soup.find('div', class_='availability').text.strip() if soup.find('div', class_='availability') else None,
        # 'image_url': soup.find('img', class_='card-image')['src'] if soup.find('img', class_='card-image') else None,
    }
    return data

def visit_card_page(card_id):
    """Visit a card page and process it"""
    url = BASE_URL.format(card_id)
    
    try:
        response = make_request(url, card_id)
        
        # Successfully got the page
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract data from the page
        card_data = extract_card_data(soup, card_id)
        
        # Store result in MongoDB
        crawl_results.update_one(
            {'card_id': card_id},
            {'$set': card_data},
            upsert=True
        )
        
        logger.debug(f"Success for card ID {card_id}!")
        return (card_id, True, card_data.get('title', 'No title'))
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error visiting card ID {card_id}: {error_msg}")
        
        # Record failure in MongoDB
        crawl_results.update_one(
            {'card_id': card_id},
            {
                '$set': {
                    'card_id': card_id,
                    'crawl_timestamp': datetime.now(),
                    'success': False,
                    'error': error_msg
                }
            },
            upsert=True
        )
        
        return (card_id, False, error_msg)

def process_batch(card_ids, crawl_id):
    """Process a batch of card IDs"""
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Create future tasks
        future_to_card = {executor.submit(visit_card_page, card_id): card_id for car