import pymongo
import pandas as pd
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import numpy as np
import time
from datetime import datetime

# Load environment variables
load_dotenv()

# Time tracking
start_time = time.time()
print(f"Script started at {datetime.now().strftime('%H:%M:%S')}")

# Connect to MongoDB
print(f"Connecting to MongoDB...")
mongo_uri = os.getenv('MONGO_URI')
client = pymongo.MongoClient(mongo_uri)
db = client.get_database()
products_collection = db.products
cards_collection = db.cards
print(f"Connected to database: {db.name}")

# Query the products with gameId=1 and groupId=97
products_query = {"gameId": 1, "groupId": 97}
products_count = products_collection.count_documents(products_query)
print(f"Found {products_count} products with gameId=1 and groupId=97")

# Get all products at once
print(f"Loading all products into memory...")
products_data = list(products_collection.find(products_query))
print(f"Loaded {len(products_data)} products into memory")

# Pre-load card data into memory for faster matching, with multiple filters
print(f"Pre-loading filtered card data for efficient matching...")
clean_name_map = {}
name_map = {}

# Build card query with all optimizations
cards_query = {
    "lang": "en",  # English cards only
    "layout": "normal",  # Normal layout cards only
    "promo": False,  # No promotional cards
    "games": "paper"  # Only cards available in paper (physical) format
}

# Count filtered cards for progress reporting
cards_count = cards_collection.count_documents(cards_query)
print(f"Building name maps from {cards_count} filtered cards...")

# Process cards in chunks to show progress
chunk_size = 25000
processed = 0

# Process cards in chunks but still build complete maps
for i in range(0, cards_count, chunk_size):
    chunk_start = time.time()
    cards_chunk = list(cards_collection.find(cards_query).skip(i).limit(chunk_size))
    processed += len(cards_chunk)
    print(f"Processing cards {processed - len(cards_chunk) + 1}-{processed} of {cards_count}...")

    for card in cards_chunk:
        card_id = card.get("_id")

        # Add to cleanName map
        if "cleanName" in card and card["cleanName"]:
            clean_name = card["cleanName"].lower()  # Use lowercase for case-insensitive matching
            if clean_name not in clean_name_map:
                clean_name_map[clean_name] = []
            clean_name_map[clean_name].append(card)

        # Add to name map
        if "name" in card and card["name"]:
            name = card["name"].lower()  # Use lowercase for case-insensitive matching
            if name not in name_map:
                name_map[name] = []
            name_map[name].append(card)

    print(f"Processed chunk in {time.time() - chunk_start:.2f} seconds")

print(f"Created maps with {len(clean_name_map)} unique cleanNames and {len(name_map)} unique names")


# Define fast match function using pre-loaded maps
def find_matching_card_fast(product):
    """Find a matching card using pre-loaded maps for speed"""

    # Try match by cleanName first (if it exists)
    if 'cleanName' in product and product['cleanName']:
        clean_name = product['cleanName'].lower()
        if clean_name in clean_name_map:
            # If there are multiple matches, try to filter by set if available
            matches = clean_name_map[clean_name]
            if len(matches) == 1:
                return matches[0], "cleanName map"
            elif 'set' in product and product['set']:
                for card in matches:
                    if card.get('set') == product['set']:
                        return card, "cleanName+set map"
                # If no exact set match, return first match
                return matches[0], "cleanName map (multiple)"
            else:
                return matches[0], "cleanName map (first)"

    # Try match by name
    if 'name' in product and product['name']:
        name = product['name'].lower()
        if name in name_map:
            # If there are multiple matches, try to filter by set if available
            matches = name_map[name]
            if len(matches) == 1:
                return matches[0], "name map"
            elif 'set' in product and product['set']:
                for card in matches:
                    if card.get('set') == product['set']:
                        return card, "name+set map"
                # If no exact set match, return first match
                return matches[0], "name map (multiple)"
            else:
                return matches[0], "name map (first)"

    # No match found
    return None, "no match"


def extract_prices(prices_data):
    """Safely extract and convert price data"""
    prices = {}

    # Handle non-dict prices
    if not isinstance(prices_data, dict):
        return prices

    # Regular prices
    for currency in ['usd', 'eur']:
        try:
            if currency in prices_data and prices_data[currency]:
                prices[currency] = float(prices_data[currency]) if isinstance(prices_data[currency], str) else \
                    prices_data[currency]
        except (ValueError, TypeError):
            prices[currency] = None

    # Foil prices
    if 'foil' in prices_data and isinstance(prices_data['foil'], dict):
        for currency in ['usd', 'eur']:
            try:
                if currency in prices_data['foil'] and prices_data['foil'][currency]:
                    prices[f'foil_{currency}'] = float(prices_data['foil'][currency]) if isinstance(
                        prices_data['foil'][currency], str) else prices_data['foil'][currency]
            except (ValueError, TypeError):
                prices[f'foil_{currency}'] = None

    return prices


# Process all products
print(f"\nProcessing all {len(products_data)} products...")
start_processing = time.time()

combined_data = []
match_methods = {}
total_matched = 0

for i, product in enumerate(products_data):
    # Print progress every 50 products
    if (i + 1) % 50 == 0 or i == 0 or i == len(products_data) - 1:
        print(f"Processing product {i + 1}/{len(products_data)}...")

    # Create base record
    record = {
        "product_id": str(product.get("_id")),
        "product_name": product.get("name", ""),
        "product_clean_name": product.get("cleanName", ""),
        "set": product.get("setName", product.get("set", "")),
        "rarity": product.get("rarity", ""),
        "product_price": product.get("price", None)
    }

    # Find matching card using fast method
    matching_card, match_method = find_matching_card_fast(product)

    # Update match statistics
    if matching_card:
        total_matched += 1
        match_methods[match_method] = match_methods.get(match_method, 0) + 1

    # Add card data to record
    if matching_card:
        record["card_id"] = str(matching_card.get("_id"))
        record["card_name"] = matching_card.get("name", "")
        record["card_clean_name"] = matching_card.get("cleanName", "")
        record["match_method"] = match_method

        # Extract prices
        prices = extract_prices(matching_card.get("prices", {}))
        record.update({
            "card_price_usd": prices.get("usd"),
            "card_price_eur": prices.get("eur"),
            "card_price_foil_usd": prices.get("foil_usd"),
            "card_price_foil_eur": prices.get("foil_eur")
        })

        # Calculate additional price metrics if both prices are available
        if record["product_price"] is not None and record["card_price_usd"] is not None:
            # Calculate price difference
            product_price = record["product_price"]
            card_price = record["card_price_usd"]
            delta_price = product_price - card_price

            # Determine the lower price
            low_price = min(product_price, card_price)

            # Calculate the buy indicator (lowPrice / deltaPrice)
            # Handle division by zero or near-zero
            if abs(delta_price) < 0.01:  # If prices are almost identical
                record["buy_indicator"] = 0  # No advantage
            else:
                record["buy_indicator"] = low_price / abs(delta_price)

            # Calculate percent difference from delta
            if card_price > 0:
                record["percent_off_delta"] = (delta_price / card_price) * 100
            else:
                record["percent_off_delta"] = 0
        else:
            record["buy_indicator"] = None
            record["percent_off_delta"] = None

    else:
        record.update({
            "card_id": None,
            "card_name": None,
            "card_clean_name": None,
            "match_method": match_method,
            "card_price_usd": None,
            "card_price_eur": None,
            "card_price_foil_usd": None,
            "card_price_foil_eur": None,
            "buy_indicator": None,
            "percent_off_delta": None
        })

    combined_data.append(record)

# Calculate total processing time
processing_time = time.time() - start_processing
print(
    f"Processed all products in {processing_time:.2f} seconds ({processing_time / len(products_data):.4f} seconds per product)")

# Create DataFrame
combined_df = pd.DataFrame(combined_data)

# Print match statistics
print(f"\nMatch statistics:")
print(f"Total products: {len(products_data)}")
print(f"Products with matching cards: {total_matched}")
print(f"Match rate: {(total_matched / len(products_data)) * 100:.2f}%")
print("\nMatch methods used:")
for method, count in sorted(match_methods.items(), key=lambda x: x[1], reverse=True):
    print(f"- {method}: {count}")

# Display basic information about the results
print(f"\nCombined data: {len(combined_df)} records")
print(f"Products with matching cards: {combined_df['card_id'].notnull().sum()}")
print(f"Products with USD prices: {combined_df['card_price_usd'].notnull().sum()}")
print(f"Products with product prices: {combined_df['product_price'].notnull().sum()}")
print(f"Products with buy indicator values: {combined_df['buy_indicator'].notnull().sum()}")

# Summary statistics of the buy indicators
buy_indicators = combined_df['buy_indicator'].dropna()
if len(buy_indicators) > 0:
    print("\nBuy Indicator Statistics:")
    print(f"Mean: {buy_indicators.mean():.4f}")
    print(f"Median: {buy_indicators.median():.4f}")
    print(f"Min: {buy_indicators.min():.4f}")
    print(f"Max: {buy_indicators.max():.4f}")

# Save to CSV file
output_file = 'combined_card_prices.csv'
combined_df.to_csv(output_file, index=False)
print(f"\nCombined data saved to {output_file}")

# Only create visualizations if we have enough matched data points
price_df = combined_df.dropna(subset=['product_price', 'card_price_usd'])
if len(price_df) >= 5:
    print(f"\nCreating visualizations for {len(price_df)} matched products with prices...")

    # Price comparison statistics
    price_diff = price_df['product_price'] - price_df['card_price_usd']
    print("\nPrice comparison (product price vs card USD price):")
    print(f"Mean difference: ${price_diff.mean():.2f}")
    print(f"Median difference: ${price_diff.median():.2f}")
    print(f"Standard deviation: ${price_diff.std():.2f}")
    print(f"Min difference: ${price_diff.min():.2f}")
    print(f"Max difference: ${price_diff.max():.2f}")

    # Correlation
    corr = price_df[['product_price', 'card_price_usd']].corr()
    print(f"Correlation between product price and card USD price: {corr.iloc[0, 1]:.4f}")

    # Create scatter plot
    plt.figure(figsize=(10, 6))
    plt.scatter(price_df['card_price_usd'], price_df['product_price'], alpha=0.6)
    plt.title('Card USD Price vs Product Price')
    plt.xlabel('Card USD Price ($)')
    plt.ylabel('Product Price ($)')
    plt.grid(True, alpha=0.3)

    # Add identity line (y=x)
    max_price = max(price_df['card_price_usd'].max(), price_df['product_price'].max())
    plt.plot([0, max_price], [0, max_price], 'r--', alpha=0.7)

    # Add regression line
    if len(price_df) > 1:
        z = np.polyfit(price_df['card_price_usd'], price_df['product_price'], 1)
        p = np.poly1d(z)
        plt.plot(price_df['card_price_usd'], p(price_df['card_price_usd']), 'g-', alpha=0.7)
        plt.legend(['y=x', f'y={z[0]:.2f}x+{z[1]:.2f}'])

    plt.tight_layout()
    plt.savefig('price_comparison.png')
    print("Price comparison chart saved to price_comparison.png")

    # Create histogram of price differences
    plt.figure(figsize=(10, 6))
    plt.hist(price_diff, bins=20, alpha=0.7)
    plt.axvline(x=0, color='r', linestyle='--')
    plt.title('Distribution of Price Differences (Product Price - Card USD Price)')
    plt.xlabel('Price Difference ($)')
    plt.ylabel('Frequency')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('price_difference_histogram.png')
    print("Price difference histogram saved to price_difference_histogram.png")

    # Create additional charts for new metrics if we have enough data
    buy_indicator_df = price_df.dropna(subset=['buy_indicator'])
    if len(buy_indicator_df) >= 5:
        # Create histogram of buy indicators
        plt.figure(figsize=(10, 6))
        plt.hist(buy_indicator_df['buy_indicator'], bins=20, alpha=0.7)
        plt.title('Distribution of Buy Indicators (lowPrice / deltaPrice)')
        plt.xlabel('Buy Indicator Value')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('buy_indicator_histogram.png')
        print("Buy indicator histogram saved to buy_indicator_histogram.png")

        # Create scatter plot of buy indicator vs price difference
        plt.figure(figsize=(10, 6))
        price_diff_abs = buy_indicator_df['product_price'] - buy_indicator_df['card_price_usd']
        plt.scatter(price_diff_abs, buy_indicator_df['buy_indicator'], alpha=0.6)
        plt.title('Price Difference vs Buy Indicator')
        plt.xlabel('Price Difference ($)')
        plt.ylabel('Buy Indicator Value')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('price_diff_vs_buy_indicator.png')
        print("Price difference vs buy indicator scatter plot saved to price_diff_vs_buy_indicator.png")

# Report total execution time
total_time = time.time() - start_time
print(f"\nTotal script execution time: {total_time:.2f} seconds")
print(f"Script completed at {datetime.now().strftime('%H:%M:%S')}")