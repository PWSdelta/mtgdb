import os
import csv
import pymongo
import re
from datetime import datetime
import time

# Start timing
start_time = time.time()

# MongoDB connection with optimized settings
# client = pymongo.MongoClient(
#     "mongodb://localhost:27017/",
#     maxPoolSize=50,  # Increase connection pool
#     socketTimeoutMS=30000,  # Increase timeout
#     w=1,  # Reduce write concern for speed
#     journal=False  # Disable journaling for speed
# )

client = pymongo.MongoClient(
    os.getenv('MONGO_URI'),
    maxPoolSize=50,  # Increase connection pool
    socketTimeoutMS=30000,  # Increase timeout
    w=1,  # Reduce write concern for speed
    journal=False  # Disable journaling for speed
)
db = client["mtgdbmongo"]
collection = db["products"]

# Create compound index if it doesn't exist
# This dramatically speeds up upsert operations
if "productId_gameId_idx" not in collection.index_information():
    print("Creating compound index on productId and gameId...")
    collection.create_index(
        [("productId", pymongo.ASCENDING), ("gameId", pymongo.ASCENDING)],
        unique=True,
        background=True,
        name="productId_gameId_idx"
    )

# Path to the folder containing CSV files
csv_folder = "downloads/"  # Replace with your actual folder path


def import_csv_files(folder_path):
    # Get a list of all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    if not csv_files:
        print("No CSV files found in the specified folder.")
        return

    total_processed = 0
    total_inserted = 0
    total_updated = 0

    for csv_file in csv_files:
        file_start_time = time.time()

        # Extract game_id and group_id from filename using regex
        filename_pattern = r"ProductsAndPrices_game_(\d+)_group_(\d+)"
        match = re.search(filename_pattern, csv_file)

        if match:
            game_id = int(match.group(1))
            group_id = int(match.group(2))
            print(f"Processing file: {csv_file} (game_id: {game_id}, group_id: {group_id})")
        else:
            print(f"Could not extract game_id and group_id from filename: {csv_file}")
            print("Skipping this file. Expected format: ProductsAndPrices_game_X_group_Y")
            continue

        file_path = os.path.join(folder_path, csv_file)
        current_time = datetime.now()

        # Create a bulk operations object
        bulk_operations = []
        processed_count = 0

        # Increased batch size for better performance
        # MongoDB can handle much larger batches than 3000 documents
        batch_size = 10000

        # Read the CSV file
        with open(file_path, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)

            # Process rows in batches for better performance
            for row in csv_reader:
                # Convert string values to appropriate types where possible
                processed_row = {}
                for key, value in row.items():
                    # Skip empty values
                    if value == '':
                        continue

                    # Try to convert numeric values
                    if value.replace('.', '', 1).isdigit():
                        # Check if it's a float or int
                        if '.' in value:
                            processed_row[key] = float(value)
                        else:
                            processed_row[key] = int(value)
                    else:
                        processed_row[key] = value

                # Add game_id and group_id from filename
                processed_row['gameId'] = game_id

                # Add import metadata
                processed_row['import_date'] = current_time
                processed_row['source_file'] = csv_file

                # Determine unique identifier for upsert
                if 'productId' in processed_row:
                    filter_criteria = {
                        'productId': processed_row['productId'],
                        'gameId': game_id
                    }

                    # Add to bulk operations
                    bulk_operations.append(
                        pymongo.UpdateOne(
                            filter_criteria,
                            {'$set': processed_row},
                            upsert=True
                        )
                    )

                    processed_count += 1

                    if len(bulk_operations) >= batch_size:
                        # Set ordered=False for parallel processing of updates
                        results = collection.bulk_write(bulk_operations, ordered=False)
                        total_inserted += results.upserted_count
                        total_updated += results.modified_count

                        print(f"Bulk processed {len(bulk_operations)} records...")
                        bulk_operations = []
                else:
                    print(f"Warning: Record missing productId, skipping: {processed_row}")

            # Process any remaining operations
            if bulk_operations:
                results = collection.bulk_write(bulk_operations, ordered=False)
                total_inserted += results.upserted_count
                total_updated += results.modified_count

            total_processed += processed_count
            file_end_time = time.time()
            file_duration = file_end_time - file_start_time
            records_per_second = processed_count / file_duration if file_duration > 0 else 0

            print(f"Processed {processed_count} records from {csv_file} in {file_duration:.2f} seconds")
            print(f"Performance: {records_per_second:.2f} records/second")

    end_time = time.time()
    total_duration = end_time - start_time
    print(f"Import complete! Total records processed: {total_processed}")
    print(f"  - {total_inserted} new records inserted")
    print(f"  - {total_updated} existing records updated")
    print(f"Total execution time: {total_duration:.2f} seconds")
    print(f"Overall performance: {total_processed / total_duration:.2f} records/second")


# Add this function to analyze MongoDB collection
def analyze_mongodb_performance():
    print("\nAnalyzing MongoDB collection...")
    # Check size of collection
    stats = db.command("collStats", "products")
    size_mb = stats["size"] / (1024 * 1024)
    storage_size_mb = stats["storageSize"] / (1024 * 1024)
    print(f"Collection size: {size_mb:.2f} MB")
    print(f"Storage size: {storage_size_mb:.2f} MB")

    # Check index sizes
    index_sizes = stats.get("indexSizes", {})
    for idx_name, idx_size in index_sizes.items():
        idx_size_mb = idx_size / (1024 * 1024)
        print(f"Index '{idx_name}' size: {idx_size_mb:.2f} MB")

    # Check indexes
    indexes = list(collection.list_indexes())
    print(f"Number of indexes: {len(indexes)}")
    for idx in indexes:
        print(f"  - {idx['name']}: {idx['key']}")

    # Document count
    doc_count = collection.count_documents({})
    print(f"Document count: {doc_count}")


# Run the import
try:
    import_csv_files(csv_folder)
    analyze_mongodb_performance()
except Exception as e:
    print(f"Error during import: {str(e)}")
finally:
    client.close()
    print("MongoDB connection closed.")