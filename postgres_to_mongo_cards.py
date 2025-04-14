import pymongo
from sqlalchemy import create_engine, text
import pandas as pd
import time
import math

# PostgreSQL Configuration
pg_connection_string = "postgresql://postgres:asdfghjkl@localhost:5432/mtgdb"

# MongoDB Configuration
mongo_uri = "mongodb://localhost:27017/"
mongo_db_name = "mtgdbmongo"
mongo_collection_name = "cards"


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
    initial_chunk_size = math.ceil(total_rows / 31)
    chunk_size = next_prime(initial_chunk_size)
    return chunk_size


def transfer_data_chunk(chunk):
    try:
        client = pymongo.MongoClient(mongo_uri)
        db = client[mongo_db_name]
        collection = db[mongo_collection_name]

        if len(chunk) == 0:
            print("Warning: Empty chunk, skipping insert")
            return

        result = collection.insert_many(chunk)
        print(f"Inserted {len(result.inserted_ids)} records into MongoDB.")

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

            print(f"Processing {len(chunk_df)} rows...")
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

        print("Data transfer completed")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()