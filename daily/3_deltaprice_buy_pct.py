import pymongo
import numpy as np
from dotenv import load_dotenv
import os
import pandas as pd
import time
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Load environment variables
load_dotenv()

start_time = time.time()
logger.info("Connecting to MongoDB...")
mongo_uri = os.getenv('MONGO_URI')
client = pymongo.MongoClient(mongo_uri,
                             maxPoolSize=100,  # Increase connection pool size
                             connectTimeoutMS=30000,
                             socketTimeoutMS=None,
                             serverSelectionTimeoutMS=30000)
db = client.get_database()
products_collection = db.products
logger.info(f"Connected to database: {db.name}")

# Calculate delta and global price values based on product price data
def calculate_price_values(product):
    delta_price_values = []
    global_price_values = []
    result = {}

    # Get product price fields
    if product:
        # Handle lowPrice
        low_price = None
        if 'lowPrice' in product and product['lowPrice'] is not None:
            try:
                low_price = float(product['lowPrice']) if isinstance(product['lowPrice'], str) else float(
                    product['lowPrice'])
                delta_price_values.append(low_price)  # Add lowPrice once for delta
                delta_price_values.append(low_price)  # Add lowPrice twice for delta (as per formula)

                # No longer adding lowPrice to global_price_values
            except (ValueError, TypeError):
                pass

        # Handle marketPrice
        market_price = None
        if 'marketPrice' in product and product['marketPrice'] is not None:
            try:
                market_price = float(product['marketPrice']) if isinstance(product['marketPrice'], str) else float(
                    product['marketPrice'])
                delta_price_values.append(market_price)  # For deltaPrice
                global_price_values.append(market_price)  # For globalPrice
            except (ValueError, TypeError):
                pass

        # Handle midPrice
        mid_price = None
        if 'midPrice' in product and product['midPrice'] is not None:
            try:
                mid_price = float(product['midPrice']) if isinstance(product['midPrice'], str) else float(
                    product['midPrice'])
                delta_price_values.append(mid_price)  # For deltaPrice

                # No longer adding midPrice to global_price_values
            except (ValueError, TypeError):
                pass

        # Calculate original deltaPrice for use in globalPrice calculation
        original_delta_price = None
        if len(delta_price_values) > 0:
            original_delta_price = np.mean(delta_price_values)
            # Store the count of values used (for statistical purposes)
            result['deltaPrice_values_used'] = len(delta_price_values)

        # For globalPrice calculation, include the original deltaPrice
        if original_delta_price is not None:
            global_price_values.append(original_delta_price)

        # Add card pricing data if available
        if 'card' in product and product['card'] is not None:
            # Add USD price if normal
            if 'prices' in product['card'] and product['card']['prices'] is not None:
                if 'usd' in product['card']['prices'] and product['card']['prices']['usd'] is not None:
                    try:
                        usd_price = float(product['card']['prices']['usd'])
                        # In a real scenario, you'd have logic to determine if it's "normal"
                        # For now we'll just include it if it's a positive number
                        if usd_price > 0:
                            global_price_values.append(usd_price)
                    except (ValueError, TypeError):
                        pass

                # Add EUR price if available
                if 'eur' in product['card']['prices'] and product['card']['prices']['eur'] is not None:
                    try:
                        eur_price = float(product['card']['prices']['eur'])
                        # Convert EUR to USD if needed using an exchange rate
                        # For simplicity, let's assume 1 EUR = 1.1 USD approximately
                        eur_to_usd_rate = 1.1
                        global_price_values.append(eur_price * eur_to_usd_rate)
                    except (ValueError, TypeError):
                        pass

        # Calculate globalPrice if we have values
        if len(global_price_values) > 0:
            # Store the global price
            global_price = np.mean(global_price_values)
            result['globalPrice'] = global_price
            result['globalPrice_values_used'] = len(global_price_values)

            # Set deltaPrice equal to globalPrice
            result['deltaPrice'] = global_price

        # Calculate deltaRatio if we have lowPrice and globalPrice
        if low_price is not None and 'globalPrice' in result and result['globalPrice'] is not None and result[
            'globalPrice'] != 0:
            result['deltaRatio'] = low_price / result['globalPrice']

    return result if result else None

logger.info("Starting price calculations for all products...")

# Get total count for progress reporting
total_products = products_collection.count_documents({})
logger.info(f"Found {total_products} products to process")

# Statistics tracking
stats = {
    "updated": 0,
    "skipped": 0,
    "delta_values_used": {},
    "global_values_used": {},
    "negative_delta": 0,
    "zero_delta": 0,
    "positive_delta": 0,
    "with_ratio": 0,
    "with_global_price": 0
}

# Process in batches for better performance
batch_size = 8831
total_batches = (total_products + batch_size - 1) // batch_size
processed_count = 0

for batch_num in range(total_batches):
    batch_start = time.time()

    # Get batch of products
    products = list(products_collection.find().skip(batch_num * batch_size).limit(batch_size))
    batch_updates = []

    for product in products:
        product_id = product.get("productId")

        # Calculate price values
        price_values = calculate_price_values(product)

        if price_values:
            # Add to batch update list
            batch_updates.append(
                pymongo.UpdateOne(
                    {"_id": product["_id"]},
                    {"$set": price_values}
                )
            )

            # Update statistics
            stats["updated"] += 1

            if "deltaPrice" in price_values:
                values_used = price_values.get("deltaPrice_values_used", 0)
                stats["delta_values_used"][values_used] = stats["delta_values_used"].get(values_used, 0) + 1

                # Track delta value stats
                delta_price = price_values["deltaPrice"]
                if delta_price < 0:
                    stats["negative_delta"] += 1
                elif delta_price == 0:
                    stats["zero_delta"] += 1
                else:
                    stats["positive_delta"] += 1

            if "deltaRatio" in price_values:
                stats["with_ratio"] += 1

            if "globalPrice" in price_values:
                stats["with_global_price"] += 1
                values_used = price_values.get("globalPrice_values_used", 0)
                stats["global_values_used"][values_used] = stats["global_values_used"].get(values_used, 0) + 1
        else:
            stats["skipped"] += 1

        processed_count += 1

    # Execute batch update
    if batch_updates:
        result = products_collection.bulk_write(batch_updates, ordered=False)

    # Progress reporting
    batch_end = time.time()
    batch_duration = batch_end - batch_start
    progress = processed_count / total_products * 100

    # Calculate ETA
    elapsed_time = batch_end - start_time
    estimated_total_time = elapsed_time * (total_products / processed_count)
    eta = start_time + estimated_total_time
    eta_str = datetime.fromtimestamp(eta).strftime('%Y-%m-%d %H:%M:%S')

    logger.info(f"Batch {batch_num + 1}/{total_batches} completed in {batch_duration:.2f}s - "
                f"Progress: {progress:.2f}% ({processed_count}/{total_products}) - "
                f"ETA: {eta_str}")

# Calculate final statistics
end_time = time.time()
total_duration = end_time - start_time
avg_rate = processed_count / total_duration

logger.info("\n" + "=" * 50)
logger.info("PRICE VALUES UPDATE COMPLETED")
logger.info("=" * 50)
logger.info(f"Total time: {timedelta(seconds=int(total_duration))}")
logger.info(f"Average processing rate: {avg_rate:.2f} products/second")
logger.info("\nResults:")
logger.info(f"Total products processed: {processed_count}")
logger.info(f"Products updated: {stats['updated']}")
logger.info(f"Products skipped: {stats['skipped']}")
logger.info(f"Products with deltaRatio: {stats['with_ratio']}")
logger.info(f"Products with globalPrice: {stats['with_global_price']}")
logger.info("\nDelta price statistics:")
logger.info(f"Negative delta values: {stats['negative_delta']}")
logger.info(f"Zero delta values: {stats['zero_delta']}")
logger.info(f"Positive delta values: {stats['positive_delta']}")
logger.info("\nValues used for deltaPrice calculation:")
for count, frequency in sorted(stats["delta_values_used"].items()):
    logger.info(f"  {count} values: {frequency} products ({frequency / stats['updated'] * 100:.2f}%)")
logger.info("\nValues used for globalPrice calculation:")
for count, frequency in sorted(stats["global_values_used"].items()):
    percentage = frequency / stats['with_global_price'] * 100 if stats['with_global_price'] > 0 else 0
    logger.info(f"  {count} values: {frequency} products ({percentage:.2f}%)")

# Create a sample of products with different values and worth more than $3
logger.info("\nCreating sample data for verification...")

sample_data = []

# Get samples with different characteristics, filtering for items worth more than $3
min_price = 3  # $3 minimum value
sample_queries = [
    {"deltaPrice": {"$lt": 0}, "marketPrice": {"$gt": min_price}},  # Negative delta
    {"deltaPrice": 0, "marketPrice": {"$gt": min_price}},  # Zero delta
    {"deltaPrice": {"$gt": 0}, "marketPrice": {"$gt": min_price}},  # Positive delta
]

for query in sample_queries:
    for product in products_collection.find(query).limit(333):  # 333 samples from each category
        product_id = product.get("productId")

        # Extract pricing information
        sample_item = {
            "product_id": product_id,
            "product_name": product.get("name", "Unknown"),
            "deltaPrice": product.get("deltaPrice"),
            "deltaRatio": product.get("deltaRatio"),
            "globalPrice": product.get("globalPrice"),
            "delta_values_used": product.get("deltaPrice_values_used"),
            "global_values_used": product.get("globalPrice_values_used"),
            "lowPrice": product.get("lowPrice"),
            "marketPrice": product.get("marketPrice"),
            "midPrice": product.get("midPrice"),
        }

        # Add card prices if available
        if 'card' in product and product['card'] and 'prices' in product['card']:
            if 'usd' in product['card']['prices']:
                sample_item['card_price_usd'] = product['card']['prices']['usd']
            if 'eur' in product['card']['prices']:
                sample_item['card_price_eur'] = product['card']['prices']['eur']

        sample_data.append(sample_item)

# Save sample to CSV for verification
sample_df = pd.DataFrame(sample_data)
sample_file = 'price_values_sample.csv'
sample_df.to_csv(sample_file, index=False)
logger.info(f"Saved sample data to {sample_file} (only items worth more than ${min_price})")

logger.info("\nProcess complete!")