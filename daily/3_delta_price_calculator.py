import pymongo
import pandas as pd
import time
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB connection
client = pymongo.MongoClient(os.getenv("MONGO_URI"))
db = client["mtgdbmongo"]
products_collection = db["products"]
cards_collection = db["cards"]


def fast_price_metrics_calculation():
    start_time = time.time()
    print("Starting fast price metrics calculation...")

    # Identify non-foil products by their subTypeName
    print("Identifying non-foil products...")

    # Projection to get only the fields we need
    projection = {
        "_id": 1,
        "productId": 1,
        "name": 1,
        "subTypeName": 1,
        "marketPrice": 1,
        "lowPrice": 1
    }

    # Query for non-foil products
    print("Loading non-foil products...")

    # Query where subTypeName is not "Foil" or doesn't exist
    non_foil_query = {
        "$or": [
            {"subTypeName": {"$ne": "Foil"}},
            {"subTypeName": {"$exists": False}}
        ]
    }

    # Get the cursor for non-foil products
    cursor = products_collection.find(non_foil_query, projection)

    # Convert to DataFrame
    df = pd.DataFrame(list(cursor))

    if df.empty:
        print("No non-foil products found. Please check your collection and filtering criteria.")
        return

    print(f"Loaded {len(df)} non-foil products into DataFrame")

    # Calculate deltaPrice as mean of marketPrice and lowPrice only
    print("Calculating deltaPrice...")
    df['deltaPrice'] = df[['marketPrice', 'lowPrice']].mean(axis=1)

    # Print a sample for validation
    print("\nValidation sample (5 random records):")
    sample_fields = ['productId', 'name', 'subTypeName', 'marketPrice', 'lowPrice', 'deltaPrice']
    sample_fields = [field for field in sample_fields if field in df.columns]
    sample_df = df.sample(min(5, len(df)))[sample_fields]
    print(sample_df)

    # Calculate buyIndicator
    print("Calculating buyIndicator...")

    # Calculate reference price (mean of marketPrice and deltaPrice)
    # Only where both marketPrice and lowPrice are available
    mask = df['marketPrice'].notna() & df['lowPrice'].notna()
    df.loc[mask, 'reference_price'] = df.loc[mask, ['marketPrice', 'deltaPrice']].mean(axis=1)

    # Calculate buyIndicator as percentage variance of lowPrice from reference_price
    df.loc[mask, 'buyIndicator'] = ((df.loc[mask, 'lowPrice'] - df.loc[mask, 'reference_price']) /
                                    df.loc[mask, 'reference_price']) * 100

    # Prepare bulk updates
    print("Preparing bulk update operations...")

    # Create a list of update operations
    updates = []

    # Create a record of which fields to update for each document
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Creating update operations"):
        update_fields = {}

        if pd.notna(row.get('deltaPrice')):
            update_fields['deltaPrice'] = float(row['deltaPrice'])

        if pd.notna(row.get('buyIndicator')):
            update_fields['buyIndicator'] = float(row['buyIndicator'])

        if update_fields:
            updates.append(pymongo.UpdateOne(
                {"_id": row["_id"]},
                {"$set": update_fields}
            ))

        # Process in batches to avoid memory issues
        if len(updates) >= 10000:
            products_collection.bulk_write(updates)
            updates = []

    # Process any remaining updates
    if updates:
        products_collection.bulk_write(updates)

    # Create indices if they don't exist
    print("Creating indices...")
    products_collection.create_index("deltaPrice")
    products_collection.create_index("buyIndicator")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nTotal execution time: {execution_time:.2f} seconds")


def verify_metrics_sample():
    """Verify metrics on a small sample of products"""
    print("\nVerifying metrics on 5 random products...")

    # Query for non-foil products with metrics
    non_foil_query = {
        "$or": [
            {"subTypeName": {"$ne": "Foil"}},
            {"subTypeName": {"$exists": False}}
        ],
        "deltaPrice": {"$exists": True}
    }

    sample_products = products_collection.find(non_foil_query).limit(17)

    for product in sample_products:
        product_id = product.get('productId', 'N/A')
        product_name = product.get('name', 'Unknown Product')
        sub_type = product.get('subTypeName', 'N/A')
        delta_price = product.get('deltaPrice')
        buy_indicator = product.get('buyIndicator')

        print(f"\nProduct: {product_name} (ID: {product_id})")
        print(f"Type: {sub_type}")
        print(f"deltaPrice: ${delta_price:.2f}" if delta_price is not None else "deltaPrice: Not available")

        if buy_indicator is not None:
            print(f"buyIndicator: {buy_indicator:.2f}%")
            if buy_indicator < -40:
                print("Interpretation: GOOD BUY (>40% below reference price)")
            elif buy_indicator <= 0:
                print("Interpretation: NEUTRAL (0-10% below reference price)")
            else:
                print("Interpretation: POOR BUY (above reference price)")


def find_best_buying_opportunities():
    """Find and display the top buying opportunities"""
    print("\nFinding top 10 buying opportunities (products with lowPrice >= $5)...")

    # Query for best non-foil buying opportunities
    query = {
        "$or": [
            {"subTypeName": {"$ne": "Foil"}},
            {"subTypeName": {"$exists": False}}
        ],
        "lowPrice": {"$gte": 5},
        "buyIndicator": {"$lte": -50}
    }

    opportunities = products_collection.find(query).sort("buyIndicator", 1).limit(10)

    print("\nBest Buying Opportunities:")
    print("==========================")

    for i, product in enumerate(opportunities, 1):
        print(f"{i}. {product.get('name', 'Unknown Product')}")
        print(f"   - Product ID: {product.get('productId', 'N/A')}")
        print(f"   - Type: {product.get('subTypeName', 'N/A')}")
        print(f"   - Low Price: ${product.get('lowPrice', 0):.2f}")
        print(f"   - Market Price: ${product.get('marketPrice', 0):.2f}")
        print(f"   - Delta Price: ${product.get('deltaPrice', 0):.2f}")
        print(f"   - Buy Indicator: {product.get('buyIndicator', 0):.2f}%")
        print(f"   - Potential Savings: ${abs(product.get('lowPrice', 0) - product.get('marketPrice', 0)):.2f}")
        print()


if __name__ == "__main__":
    print("Fast Price Metrics Calculator")
    print("============================")

    # Run the fast calculation
    fast_price_metrics_calculation()

    # Verify the results
    verify_metrics_sample()

    # Show some valuable output
    find_best_buying_opportunities()

    print("\nMetrics calculation completed successfully!")
    print("You can now query the database to find products with good buying opportunities.")
    print("Example query:")
    print('''
    // Find non-foil products with good buying opportunities
    db.products.find({
      $or: [
        {subTypeName: {$ne: "Foil"}},
        {subTypeName: {$exists: false}}
      ],
      lowPrice: {$gte: 5},
      buyIndicator: {$lte: -50}
    }).sort({buyIndicator: 1}).limit(20)
    ''')