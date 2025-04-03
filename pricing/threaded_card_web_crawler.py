# ultra_simple_card_crawler.py
import requests
from sqlalchemy import create_engine, MetaData, Table, select
import time
import random
import os
import logging
import concurrent.futures
from tqdm import tqdm
import json
from datetime import datetime

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f"{log_dir}/crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
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

# Concurrent workers - reduced to minimize server load
MAX_WORKERS = 5  # Reduced from 10 to prevent overwhelming the server

# Request timeout in seconds
TIMEOUT = 20  # Increased timeout to account for slow responses

# Delay between requests (seconds)
MIN_DELAY = 0.5  # Increased minimum delay
MAX_DELAY = 1.5  # Increased maximum delay

# Delay between batches (seconds)
BATCH_DELAY = 30  # Add a 30-second pause between batches

# File to save results
RESULTS_FILE = "crawler_results.json"

# Batch size for processing
BATCH_SIZE = 500  # Smaller batches to reduce load


def get_all_card_ids():
    """Get all card IDs from the card_details table"""
    logger.info("Fetching card IDs from database...")
    with engine.connect() as connection:
        query = select(card_details.c.id)
        result = connection.execute(query)
        card_ids = [str(row[0]) for row in result]
        logger.info(f"Found {len(card_ids)} card IDs in database")
        return card_ids


def visit_card_page(card_id):
    """Visit a card page and return detailed result"""
    url = BASE_URL.format(card_id)

    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://pwsdelta.com/',
    }

    # Add a small random delay to avoid overloading the server
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    result = {
        'card_id': card_id,
        'url': url,
        'success': False,
        'status_code': None,
        'error': None,
        'response_time': None
    }

    try:
        start_time = time.time()
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        end_time = time.time()

        result['response_time'] = round(end_time - start_time, 2)
        result['status_code'] = response.status_code
        result['success'] = response.status_code == 200

        if response.status_code != 200:
            result['error'] = f"HTTP {response.status_code}"
            logger.debug(f"Card {card_id}: HTTP {response.status_code}")

    except requests.exceptions.Timeout:
        result['error'] = "Timeout"
        logger.debug(f"Card {card_id}: Timeout after {TIMEOUT}s")

    except requests.exceptions.ConnectionError as e:
        result['error'] = f"Connection Error: {str(e)}"
        logger.debug(f"Card {card_id}: Connection error - {str(e)}")

    except requests.exceptions.RequestException as e:
        result['error'] = f"Request Exception: {str(e)}"
        logger.debug(f"Card {card_id}: Request error - {str(e)}")

    except Exception as e:
        result['error'] = f"Unexpected Error: {str(e)}"
        logger.debug(f"Card {card_id}: Unexpected error - {str(e)}")

    return result


def save_results(results, file_path):
    """Save results to a JSON file"""
    with open(file_path, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"Results saved to {file_path}")


def analyze_errors(results):
    """Analyze error patterns in results"""
    error_types = {}
    status_codes = {}

    for result in results:
        if not result['success']:
            error = result.get('error', 'Unknown')
            error_types[error] = error_types.get(error, 0) + 1

            if result['status_code']:
                status_code = str(result['status_code'])
                status_codes[status_code] = status_codes.get(status_code, 0) + 1

    logger.info("Error analysis:")
    for error, count in error_types.items():
        logger.info(f"- {error}: {count}")

    logger.info("Status code distribution:")
    for code, count in status_codes.items():
        logger.info(f"- HTTP {code}: {count}")


def test_single_card(card_id):
    """Test a single card URL and print detailed information"""
    logger.info(f"Testing single card ID: {card_id}")
    result = visit_card_page(card_id)
    logger.info(f"Result: {json.dumps(result, indent=2)}")
    return result


def main():
    # Get all card IDs
    all_card_ids = get_all_card_ids()
    random.shuffle(all_card_ids)
    total_cards = len(all_card_ids)

    # Test a single card first to verify connection
    test_card = all_card_ids[0]
    logger.info(f"Testing connectivity with card ID: {test_card}")
    test_result = test_single_card(test_card)

    if not test_result['success']:
        logger.warning("Initial test failed. There might be connectivity issues.")
        user_input = input("Continue anyway? (y/n): ")
        if user_input.lower() != 'y':
            return

    logger.info(f"Starting to process {total_cards} cards with {MAX_WORKERS} workers")
    logger.info(f"Using batch size of {BATCH_SIZE} with {BATCH_DELAY}s pause between batches")

    results = []
    success_count = 0
    fail_count = 0

    start_time = time.time()

    # Load any existing results if available
    if os.path.exists(f"{RESULTS_FILE}.partial"):
        try:
            with open(f"{RESULTS_FILE}.partial", 'r') as f:
                results = json.load(f)
                success_count = sum(1 for r in results if r['success'])
                fail_count = sum(1 for r in results if not r['success'])
                logger.info(f"Loaded {len(results)} existing results: {success_count} success, {fail_count} failed")

                # Filter out card IDs we've already processed
                processed_ids = set(r['card_id'] for r in results)
                all_card_ids = [cid for cid in all_card_ids if cid not in processed_ids]
                logger.info(f"Remaining cards to process: {len(all_card_ids)}")
        except Exception as e:
            logger.warning(f"Failed to load existing results: {str(e)}")

    # Process cards in batches
    for i in range(0, len(all_card_ids), BATCH_SIZE):
        batch = all_card_ids[i:i + BATCH_SIZE]
        batch_number = i // BATCH_SIZE + 1
        total_batches = (len(all_card_ids) + BATCH_SIZE - 1) // BATCH_SIZE

        logger.info(f"Starting batch {batch_number}/{total_batches} ({len(batch)} cards)")

        # Process this batch with thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            batch_results = list(tqdm(
                executor.map(visit_card_page, batch),
                total=len(batch),
                desc=f"Processing batch {batch_number}/{total_batches}"
            ))

        results.extend(batch_results)

        # Count successes and failures
        batch_success = sum(1 for r in batch_results if r['success'])
        batch_fail = sum(1 for r in batch_results if not r['success'])

        success_count += batch_success
        fail_count += batch_fail

        # Log progress
        progress = len(results)
        logger.info(f"Batch {batch_number} complete: Success: {batch_success} | Failed: {batch_fail}")
        logger.info(f"Overall: {progress} cards processed | Success: {success_count} | Failed: {fail_count}")

        # Save intermediate results
        save_results(results, f"{RESULTS_FILE}.partial")

        # Analyze error patterns for this batch
        analyze_errors(batch_results)

        # Pause between batches to let the server recover
        if i + BATCH_SIZE < len(all_card_ids):
            logger.info(f"Pausing for {BATCH_DELAY} seconds before next batch...")
            for remaining in range(BATCH_DELAY, 0, -1):
                if remaining % 5 == 0:  # Only log every 5 seconds
                    logger.info(f"Resuming in {remaining} seconds...")
                time.sleep(1)

    end_time = time.time()
    elapsed_time = end_time - start_time

    logger.info(f"Finished! Success: {success_count} | Failed: {fail_count}")
    logger.info(f"Total time: {elapsed_time:.2f} seconds")

    # Save final results
    save_results(results, RESULTS_FILE)

    # Final error analysis
    analyze_errors(results)

    # Sample of failed URLs
    failed_results = [r for r in results if not r['success']]
    if failed_results:
        sample_size = min(5, len(failed_results))
        logger.info(f"Sample of {sample_size} failed card IDs:")
        for i, result in enumerate(random.sample(failed_results, sample_size)):
            logger.info(
                f"{i + 1}. Card ID: {result['card_id']}, Error: {result.get('error')}, Status: {result.get('status_code')}")


if __name__ == "__main__":
    main()