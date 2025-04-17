# Import necessary libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pymongo
import os
from dotenv import load_dotenv
from datetime import datetime

# Configure visualization settings
sns.set_style("whitegrid")

# Load environment variables
load_dotenv()

# Establish MongoDB connection
mongo_uri = os.getenv('MONGODB_URI')
client = pymongo.MongoClient(mongo_uri)
db = client.get_database(os.getenv('MONGODB_DATABASE'))
products_collection = db.products

print("Database connection established successfully.")

# Query products with gameId=1 and groupId=97 (corrected from 92)
query = {
    "gameId": 1,
    "groupId": 97
}

# Count the number of matching products
product_count = products_collection.count_documents(query)
print(f"Number of products with gameId=1 and groupId=97: {product_count}")

# Check if we have any products before proceeding
if product_count > 0:
    # Retrieve the products and convert to DataFrame
    products = list(products_collection.find(query))
    df = pd.DataFrame(products)

    # Display first few rows and basic information
    print("\nSample of products data:")
    print(df.head())

    print("\nDataFrame info:")
    print(df.info())

    print("\nColumn names in the dataset:")
    print(df.columns.tolist())

    # Explore basic statistics and attributes of the products
    print("\nMissing values per column:")
    print(df.isnull().sum())

    # Basic summary statistics of numerical fields
    print("\nSummary statistics for numerical attributes:")
    if not df.empty and df.select_dtypes(include=[np.number]).columns.size > 0:
        print(df.describe(include=[np.number]))
    else:
        print("No numerical data available to describe.")

    # Examine distribution of categorical variables
    print("\nDistribution of categorical variables:")
    for col in ['availability', 'condition', 'state']:
        if col in df.columns:
            print(f"\n{col.capitalize()} distribution:")
            print(df[col].value_counts())

    # Check when these products were last updated
    if 'updatedAt' in df.columns:
        df['updatedAt'] = pd.to_datetime(df['updatedAt'])
        print("\nLatest update time:", df['updatedAt'].max())
        print("Earliest update time:", df['updatedAt'].min())

    # Create initial visualizations of price distribution
    if 'price' in df.columns:
        # Histogram of prices
        plt.figure(figsize=(10, 6))
        sns.histplot(df['price'].dropna(), kde=True, bins=30)
        plt.title('Price Distribution for Products (Game 1, Group 97)')
        plt.xlabel('Price')
        plt.ylabel('Frequency')
        plt.show()

        # Box plot of prices
        plt.figure(figsize=(10, 6))
        sns.boxplot(y=df['price'].dropna())
        plt.title('Price Box Plot (Game 1, Group 97)')
        plt.ylabel('Price')
        plt.show()

        # Price statistics
        price_stats = df['price'].describe()
        print("Price Statistics:")
        print(price_stats)

        # Outlier detection using IQR
        Q1 = df['price'].quantile(0.25)
        Q3 = df['price'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers = df[(df['price'] < lower_bound) | (df['price'] > upper_bound)]
        print(f"Number of potential price outliers: {len(outliers)}")

        if len(outliers) > 0:
            print("Sample of outliers:")
            print(outliers.head())
    else:
        print("Price data not available for these products")

    # Prepare for more advanced price analysis
    if 'updatedAt' in df.columns and 'price' in df.columns:
        # Extract date components
        df['update_date'] = df['updatedAt'].dt.date
        df['update_month'] = df['updatedAt'].dt.month
        df['update_year'] = df['updatedAt'].dt.year

        # Group by date and calculate price statistics
        date_price_stats = df.groupby('update_date')['price'].agg(['mean', 'median', 'min', 'max', 'count'])

        print("Price statistics by update date:")
        print(date_price_stats.head())

        # Plot price trends if we have multiple dates
        if len(date_price_stats) > 1:
            plt.figure(figsize=(12, 6))
            plt.plot(date_price_stats.index, date_price_stats['mean'], label='Mean Price')
            plt.plot(date_price_stats.index, date_price_stats['median'], label='Median Price')
            plt.title('Price Trends Over Time (Game 1, Group 97)')
            plt.xlabel('Date')
            plt.ylabel('Price')
            plt.legend()
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
    else:
        print("Time-based price analysis not available")
else:
    print("\nNo products found with the specified criteria.")