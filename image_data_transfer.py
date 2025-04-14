import pymongo
from sqlalchemy import create_engine, text
import pandas as pd
import time
import math
import os
import base64
from bson.binary import Binary

# PostgreSQL Configuration
pg_connection_string = "postgresql://postgres:asdfghjkl@localhost:5432/mtgdb"

# MongoDB Configuration
mongo_uri = "mongodb://localhost:27017/"
mongo_db_name = "mtgdbmongo"
mongo_collection_name = "cards"

# Image paths
base_image_path = "pricing/images/"
art_crop_image_path = "pricing/art_crop/"


def is_prime(n):
    """Check if a number is prime."""
    if n <= 1:
        return False
    if n <= 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


def next_prime(n):
    """Find the nearest prime number greater than or equal to n."""
    if n <= 1:
        return 2

    # If n is already prime, return it
    if is_prime(n):
        return n

    # Otherwise, find the next prime
    prime = n
    found = False

    while not found:
        prime += 1
        if is_prime(prime):
            found = True

    return prime


def calculate_chunk_size(total_rows):
    """
    Calculate chunk size based on the formula:
    rows_in_table / 31, rounded up to the nearest prime number
    """
    initial_chunk_size = math.ceil(total_rows / 37)
    chunk_size = next_prime(initial_chunk_size)
    return chunk_size


def load_image_if_exists(image_path):
    """Load image from filesystem if it exists and return as binary data"""
    if os.path.exists(image_path):
        try:
            with open(image_path, "rb") as img_file:
                return Binary(img_file.read())
        except Exception as e:
            print(f"Error reading image {image_path}: {e}")
    return None


def process_card_images(card_dict):
    """Process and add image data to card dictionary"""
    # Process normal image
    if 'normal_image_name' in card_dict and card_dict['normal_image_name']:
        image_path = os.path.join(base_image_path, card_dict['normal_image_name'])
        image_data = load_image_if_exists(image_path)
        if image_data:
            card_dict['normal_image_data'] = image_data

    # Process art crop image
    if 'art_crop_image_name' in card_dict and card_dict['art_crop_image_name']:
        image_path = os.path.join(art_crop_image_path, card_dict['art_crop_image_name'])
        image_data = load_image_if_exists(image_path)
        if image_data:
            card_dict['art_crop_image_data'] = image_data

    return card_dict


def transfer_data_chunk(chunk):
    try:
        client = pymongo.MongoClient(mongo_uri)
        db = client[mongo_db_name]
        collection = db[mongo_collection_name]

        if len(chunk) == 0:
            print("Warning: Empty chunk, skipping insert")
            return

        # Process images for each card in the chunk
        processed_chunk = []
        for card in chunk:
            processed_card = process_card_images(card)
            processed_chunk.append(processed_card)

        result = collection.insert_many(processed_chunk)
        print(f"Inserted {len(result.inserted_ids)} records with images into MongoDB.")

    except Exception as e:
        print(f"Error inserting chunk: {e}")
    finally:
        if 'client' in locals():
            client.close()


def main():
    try:
        # Connect to Postgres
        print("Connecting to PostgreSQL...")
        engine = create_engine(pg_connection_string)

        # Test connection and get total rows
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM card_details"))
            total_rows = result.scalar()
            print(f"Total rows to process: {total_rows}")

        # Calculate chunk size based on formula
        chunk_size = calculate_chunk_size(total_rows)
        print(f"Using calculated chunk size: {chunk_size}")

        # Process in batches using offset
        offset = 0
        while True:
            print(f"Reading rows {offset} to {offset + chunk_size}...")

            # Read just a chunk of data
            query = text(f"SELECT * FROM card_details ORDER BY id LIMIT {chunk_size} OFFSET {offset}")
            with engine.connect() as conn:
                chunk_df = pd.read_sql(query, conn)

            if chunk_df.empty:
                print("No more data to process.")
                break

            print(f"Processing {len(chunk_df)} rows with images...")
            records = chunk_df.to_dict(orient='records')
            transfer_data_chunk(records)

            offset += chunk_size

            # Progress indicator
            print(
                f"Progress: {min(offset, total_rows)}/{total_rows} rows ({(min(offset, total_rows) / total_rows) * 100:.1f}%)")

            if len(chunk_df) < chunk_size:
                # Last batch
                break

            # Small delay to avoid overwhelming the databases
            time.sleep(0.5)

        print("Data transfer with images completed")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()