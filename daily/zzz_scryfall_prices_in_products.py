import pymongo
import pandas as pd
from dotenv import load_dotenv
import os
from time import time
import json

# Load environment variables
load_dotenv()

# Connect to MongoDB
mongo_uri = os.environ.get("MONGO_URI")
client = pymongo.MongoClient(mongo_uri)
db = client.get_database()

print("Connected to database:", db.name)
print("Available collections:", db.list_collection_names())

# Start timing
start_time = time()

# 1. Extract pricing data from cards collection
print("Extracting pricing data from cards collection...")
cards_pricing = list(db.cards.find(
    {"tcgplayer_id": {"$exists": True}},
    {"_id": 1, "name": 1, "set": 1, "tcgplayer_id": 1, "prices.usd": 1, "prices.eur": 1}
))

# Convert to dataframe
cards_df = pd.DataFrame(cards_pricing)
print(f"Retrieved {len(cards_df)} cards with tcgplayer_id")

# Process cards data to extract USD and EUR prices
if not cards_df.empty and 'prices' in cards_df.columns:
    # Create usdPrice and eurPrice columns
    cards_df['usdPrice'] = cards_df['prices'].apply(lambda x: x.get('usd') if x else None)
    cards_df['eurPrice'] = cards_df['prices'].apply(lambda x: x.get('eur') if x else None)

    # Convert price columns to numeric
    cards_df['usdPrice'] = pd.to_numeric(cards_df['usdPrice'], errors='coerce')
    cards_df['eurPrice'] = pd.to_numeric(cards_df['eurPrice'], errors='coerce')

    # Keep only needed columns
    cards_df = cards_df[['tcgplayer_id', 'name', 'set', 'usdPrice', 'eurPrice']]

    print(f"Prepared cards pricing data with columns: {cards_df.columns.tolist()}")
    print(f"Cards with tcgplayer_id: {cards_df['tcgplayer_id'].count()}")
    print(f"Cards with usdPrice: {cards_df['usdPrice'].count()}")
    print(f"Cards with eurPrice: {cards_df['eurPrice'].count()}")

    # Create multiple versions of tcgplayer_id for matching
    cards_df['tcgplayer_id_int'] = pd.to_numeric(cards_df['tcgplayer_id'], errors='coerce').fillna(-1).astype(int)
    cards_df['tcgplayer_id_str'] = cards_df['tcgplayer_id_int'].astype(str)

# 2. Extract product data
print("\nExtracting product data...")
products_pricing = list(db.products.find(
    {},
    {"_id": 1, "productId": 1, "name": 1, "lowPrice": 1, "midPrice": 1, "marketPrice": 1, "directLowPrice": 1}
))

# Convert to dataframe
products_df = pd.DataFrame(products_pricing)
print(f"Retrieved {len(products_df)} products")

if not products_df.empty:
    print(f"Products DataFrame columns: {products_df.columns.tolist()}")
    print(f"Products with productId: {products_df['productId'].count()}")

    # Create multiple versions of productId for matching
    products_df['productId_int'] = pd.to_numeric(products_df['productId'], errors='coerce').fillna(-1).astype(int)
    products_df['productId_str'] = products_df['productId_int'].astype(str)

# 3. Attempt matching using different methods
print("\nAttempting matches between cards and products...")

# Initialize counters for statistics
total_updates = 0
cards_with_usd = cards_df['usdPrice'].count()
cards_with_eur = cards_df['eurPrice'].count()

# Ensure both dataframes are not empty before attempting joins
if not cards_df.empty and not products_df.empty:
    # Try joining by integer versions of IDs
    merged_df = pd.merge(
        products_df,
        cards_df,
        left_on='productId_int',
        right_on='tcgplayer_id_int',
        how='left'
    )

    # Count matches found
    usd_matched = merged_df['usdPrice'].notna().sum()
    eur_matched = merged_df['eurPrice'].notna().sum()
    total_products = len(merged_df)

    print(
        f"Match results: {usd_matched} products matched with USD prices out of {total_products} ({usd_matched / total_products * 100:.2f}%)")
    print(f"EUR price matches: {eur_matched} products ({eur_matched / total_products * 100:.2f}%)")

    if usd_matched > 0 or eur_matched > 0:
        print("\nUpdating products collection with USD and EUR prices...")

        # Create a batch array for bulk updates
        bulk_updates = []

        # Keep track of matched and updated products
        updated_products = set()

        # Process each row in the merged dataframe
        for _, row in merged_df.iterrows():
            product_id = row['_id']
            usd_price = row['usdPrice']
            eur_price = row['eurPrice']

            # Only update if at least one price exists and we haven't updated this product yet
            if (pd.notna(usd_price) or pd.notna(eur_price)) and product_id not in updated_products:
                update_dict = {}
                if pd.notna(usd_price):
                    update_dict['usdPrice'] = float(usd_price)
                if pd.notna(eur_price):
                    update_dict['eurPrice'] = float(eur_price)

                # Add to bulk update array
                bulk_updates.append(
                    pymongo.UpdateOne(
                        {'_id': product_id},
                        {'$set': update_dict}
                    )
                )

                updated_products.add(product_id)

                # If we've accumulated 8013 updates, execute them as a batch
                if len(bulk_updates) >= 8013:
                    result = db.products.bulk_write(bulk_updates)
                    total_updates += result.modified_count
                    print(f"  Processed {len(bulk_updates)} updates...")
                    bulk_updates = []

        # Execute any remaining updates
        if bulk_updates:
            result = db.products.bulk_write(bulk_updates)
            total_updates += result.modified_count

        # Report final stats
        end_time = time()
        elapsed_time = end_time - start_time

        print("\n===== SUMMARY =====")
        print(f"Total cards with tcgplayer_id: {len(cards_df)}")
        print(f"Cards with USD prices: {cards_with_usd}")
        print(f"Cards with EUR prices: {cards_with_eur}")
        print(f"Total products: {len(products_df)}")
        print(f"Products matched with cards: {len(updated_products)}")
        print(f"Products updated in database: {total_updates}")
        print(f"Time elapsed: {elapsed_time:.2f} seconds")

        # Calculate match rate
        match_rate = len(updated_products) / len(products_df) * 100
        print(f"Match rate: {match_rate:.2f}%")

        # Save the merged data to CSV for further analysis
        merged_df.to_csv('products_with_card_prices.csv', index=False)
        print("Saved combined pricing data to products_with_card_prices.csv")
    else:
        print("No matches found between products and cards, no MongoDB updates made")
else:
    print("One or both collections were empty, couldn't perform the join")