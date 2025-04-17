import pymongo
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import csv

# Load environment variables
load_dotenv()

print(f"Connecting to MongoDB...")
mongo_uri = os.getenv('MONGO_URI')
client = pymongo.MongoClient(mongo_uri)
db = client.get_database()
products_collection = db.products
cards_collection = db.cards
print(f"Connected to database: {db.name}")

# Build card price mapping
print("\nBuilding card price mapping...")
card_price_map = {}

# Create the price map - fetch all cards with USD or EUR prices
cards_with_prices = db.cards.find(
    {"$or": [
        {"prices.usd": {"$exists": True}},
        {"prices.eur": {"$exists": True}}
    ]}
)

for card in cards_with_prices:
    if "productId" in card:
        product_id = card["productId"]
        prices = card.get("prices", {})

        # Get all the different price fields
        usd_price = prices.get("usd")
        eur_price = prices.get("eur")
        mid_price = card.get("midPrice")
        market_price = card.get("marketPrice")
        low_price = card.get("lowPrice")

        # Convert price values to float
        price_fields = [usd_price, eur_price, mid_price, market_price, low_price]
        price_values = []

        for price in price_fields:
            if price and isinstance(price, str):
                try:
                    price_values.append(float(price))
                except (ValueError, TypeError):
                    pass
            elif isinstance(price, (int, float)):
                price_values.append(float(price))

        # Calculate mean price if we have values
        mean_price = np.mean(price_values) if price_values else None

        card_price_map[product_id] = {
            "mean_price": mean_price,
            "name": card.get("name")
        }

print(f"Found {len(card_price_map)} cards with price information")

# Get all products with prices
products = list(db.products.find(
    {"price": {"$exists": True}},
    {"_id": 1, "productId": 1, "name": 1, "price": 1}
))

print(f"Retrieved {len(products)} products with price information")

# Process all products and calculate metrics
combined_data = []

for product in products:
    product_id = product.get("productId")
    product_name = product.get("name", "Unknown")

    # Get product price
    product_price = None
    price_field = product.get("price")

    if isinstance(price_field, (int, float)):
        product_price = float(price_field)
    elif isinstance(price_field, str):
        try:
            product_price = float(price_field.replace(',', '').replace('$', '').strip())
        except (ValueError, TypeError):
            pass

    # Get card mean price
    card_mean_price = None
    if product_id in card_price_map:
        card_mean_price = card_price_map[product_id]["mean_price"]

    # Create record
    record = {
        "product_id": product_id,
        "product_name": product_name,
        "product_price": product_price,
        "card_mean_price": card_mean_price
    }

    # Calculate additional price metrics if both prices are available
    if record["product_price"] is not None and record["card_mean_price"] is not None:
        # Calculate price difference using mean price
        product_price = record["product_price"]
        card_price = record["card_mean_price"]
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

    combined_data.append(record)

# Convert to DataFrame for easier handling
combined_df = pd.DataFrame(combined_data)

# Save to CSV file
output_file = 'combined_card_prices.csv'
combined_df.to_csv(output_file, index=False)

# Print a table of the results for all products with both prices
print("\n===== DEAL ANALYSIS TABLE =====")
print(
    f"{'Name':<30} {'Low Price':>10} {'Delta Price':>12} {'Market Price':>12} {'Buy Indicator':>15} {'% Off Delta':>12}")
print(f"{'-' * 30} {'-' * 10} {'-' * 12} {'-' * 12} {'-' * 15} {'-' * 12}")

# Filter for records that have both product and card prices
valid_records = [r for r in combined_data if r["product_price"] is not None and r["card_mean_price"] is not None]

# Sort by buy_indicator for better insights
valid_records.sort(
    key=lambda r: r.get("buy_indicator", float('inf')) if r.get("buy_indicator") is not None else float('inf'))

# Print all records
for record in valid_records:
    name = record["product_name"][:28] + ".." if len(record["product_name"]) > 30 else record["product_name"]

    product_price = record["product_price"]
    card_price = record["card_mean_price"]
    delta_price = product_price - card_price
    low_price = min(product_price, card_price)

    buy_indicator = record.get('buy_indicator')
    buy_indicator_str = f"{buy_indicator:.2f}" if buy_indicator is not None else "N/A"

    percent_off = record.get('percent_off_delta')
    percent_off_str = f"{percent_off:.2f}%" if percent_off is not None else "N/A"

    print(f"{name:<30} "
          f"${low_price:>9.2f} "
          f"${delta_price:>11.2f} "
          f"${card_price:>11.2f} "
          f"{buy_indicator_str:>15} "
          f"{percent_off_str:>11}")

print(f"\nTotal valid records: {len(valid_records)}")
print(f"Data saved to {output_file}")