# ultra_simple_card_crawler.py
import requests
from sqlalchemy import create_engine, MetaData, Table, select
import time
import random
import os
import logging
import concurrent.futures
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Database connection - only for reading card IDs
engine = create_engine(os.getenv('RW_DATABASE_URL', 'postgresql://postgres:asdfghjkl@localhost:5432/mtgdb'))
metadata = MetaData()
card_details = Table('card_details', metadata, autoload_with=engine)

# User agent rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
]

# Base URL for the card pages
BASE_URL = "https://pwsdelta.com/card/{0}"

# Concurrent workers - increase this for more speed
MAX_WORKERS = 11


def get_all_card_ids():
    """Get all card IDs from the card_details table"""
    with engine.connect() as connection:
        query = select(card_details.c.id)
        result = connection.execute(query)
        return [str(row[0]) for row in result]


def visit_card_page(card_id):
    """Visit a card page and return result"""
    url = BASE_URL.format(card_id)

    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        return card_id, response.status_code == 200
    except Exception as e:
        return card_id, False


def main():
    # Get all card IDs
    all_card_ids = get_all_card_ids()
    total_cards = len(all_card_ids)
    logger.info(f"Total cards to process: {total_cards}")

    success_count = 0
    fail_count = 0

    # Process all cards with thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(visit_card_page, card_id): card_id for card_id in all_card_ids}

        for future in tqdm(concurrent.futures.as_completed(futures), total=total_cards, desc="Processing cards"):
            card_id = futures[future]
            try:
                _, success = future.result()
                if success:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception:
                fail_count += 1

            # Print progress every 1000 cards
            if (success_count + fail_count) % 1000 == 0:
                logger.info(
                    f"Progress: {success_count + fail_count}/{total_cards} | Success: {success_count} | Failed: {fail_count}")

    logger.info(f"Finished! Success: {success_count} | Failed: {fail_count}")


if __name__ == "__main__":
    main()