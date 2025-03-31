from datetime import timedelta, datetime
import re
import math
import os
import numpy as np
from dotenv import load_dotenv
from flask import Flask
from flask import request, render_template
from sqlalchemy import create_engine, Table
from sqlalchemy import inspect
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

load_dotenv()

app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['TEMPLATES_AUTO_RELOAD'] = True


if os.getenv("APP_ENVIRONMENT", "DEV").startswith("standard"):
    engine = create_engine(os.environ.get('RW_DATABASE_URL'))
else:
    engine = create_engine(os.environ.get('LOCAL_DB_URL'))

# Automatically map the database schema
Base = automap_base()
Base.prepare(engine, reflect=True, schema="public")
inspector = inspect(engine)


# Access the automatically generated ORM class
CardDetails = Base.classes.card_details
Products = Base.classes.products
SpotPrices = Base.classes.spot_prices
SetDetails = Base.classes.set_details


ProductCategories = Table(
    'product_categories',  # Exact table name
    Base.metadata,  # Metadata tied to Base
    autoload_with=engine  # Dynamically load structure from DB
)

Session = sessionmaker(bind=engine)
session = Session()


def fetch_random_card_from_db():
    """
    Fetches a random card entry from the database.
    """
    try:
        random_card = (session.query(CardDetails)
                       .order_by(func.random())
                       .first())
        if random_card:
            print(f"Retrieved random card: {random_card.id}, {random_card.name}")
            return random_card
        else:
            print("No cards found in the database.")
            return None
    except Exception as e:
        print(f"An error occurred while fetching a random card: {e}")
        return None


def update_normal_price(card_id):
    print(f"update_normal_price called for card_id: {card_id}")

    # Fetch card and product data
    try:
        card = session.query(CardDetails).filter(CardDetails.id == card_id).first()
        if not card:
            raise ValueError(f"No card found with card_id: {card_id}")

        print(f"Card found: {card.name} | tcgplayer_id: {card.tcgplayer_id}")

        product = session.query(Products).filter(Products.productId == card.tcgplayer_id).first()
        if not product:
            raise ValueError(f"No product found with tcgplayer_id: {card.tcgplayer_id}")

        print(f"Product found: {product.name} | {product.productId}")

    except Exception as e:
        print(f"Error fetching card or product: {e}")
        return None

    # Calculate normal price
    try:
        prices = card.prices or {}
        usd_price = float(prices.get('usd', 0) or 0)
        eur_price = float(prices.get('eur', 0) or 0)

        low_price = float(product.lowPrice or 0)
        mid_price = float(product.midPrice or 0)
        market_price = float(product.marketPrice or 0)
        direct_low_price = float(product.directLowPrice or 0)

        all_prices = [usd_price, eur_price, low_price, mid_price, market_price, direct_low_price]
        valid_prices = [price for price in all_prices if price > 0]

        if not valid_prices:
            raise ValueError(f"No valid prices found for card_id: {card_id}")

        mean_price = float(np.mean(valid_prices))
        print(f"Calculated mean price for card {card.name}: {mean_price}")

    except Exception as e:
        print(f"Error calculating normal price: {e}")
        return None

    # Update the database
    try:
        print(f"Updating normal_price for card_id {card.id}")
        print(f"Old normal_price: {card.normal_price}")

        card.normal_price = round(mean_price, 2)
        print(f"New normal_price: {card.normal_price}")

        session.commit()
        print(f"Successfully updated normal_price for card_id {card.id}")

    except Exception as e:
        print(f"Error during database update, rolling back: {e}")
        session.rollback()
        return None

    # Return the updated card
    return card


# def insert_spot_price(Session, card, spot_price):
#     """
#     Inserts a new spot price into the database for the given CardDetails instance.
#
#     Args:
#         Session: An instance of the session factory created by sessionmaker.
#         card: An instance of the CardDetails automapped class.
#         spot_price (float): The new spot price to log.
#
#     Returns:
#         str: A message indicating success or why logging was skipped.
#     """
#     # Ensure the card instance is valid
#     if not card:
#         raise ValueError("A valid CardDetails instance must be provided.")
#
#     # Ensure the spot_price is a valid number
#     if spot_price is None or spot_price < 0:
#         raise ValueError("A valid, positive spot_price must be provided.")
#
#     # Create a session instance
#     session = Session()  # Initialize the session
#     try:
#         # Extract details from the card instance
#         card_id = card.id
#         tcgplayer_id = card.tcgplayer_id
#
#         # Query for the most recent spot price for this card
#         last_spot_price = session.query(SpotPrices).filter(
#             (SpotPrices.card_id == card_id) | (SpotPrices.tcgplayer_id == tcgplayer_id)
#         ).order_by(SpotPrices.date_created.desc()).first()
#
#         # Check if the most recent spot price was added within 12 hours
#         if last_spot_price and last_spot_price.date_created > datetime.utcnow() - timedelta(hours=12):
#             return f"No new spot price logged. Last price added less than 12 hours ago (at {last_spot_price.date_created})."
#
#         # Create a new spot price entry
#         new_spot_price = SpotPrices(
#             card_id=card_id,
#             tcgplayer_id=tcgplayer_id,
#             spot_price=spot_price,
#             date_created=datetime.utcnow()
#         )
#
#         # Add the new spot price to the database and commit
#         session.add(new_spot_price)
#         session.commit()
#         print(f"New spot price logged for card_id: {card_id}, tcgplayer_id: {tcgplayer_id}, with price: {spot_price}.")
#
#         return f"New spot price logged for card_id: {card_id}, tcgplayer_id: {tcgplayer_id}, with price: {spot_price}."
#
#     except Exception as e:
#         # Roll back the transaction on error
#         session.rollback()
#         raise RuntimeError(f"Error inserting spot price: {e}")
#
#     finally:
#         session.close()


# def update_all_spot_prices(Session, update_normal_price, insert_spot_price):
#     """
#     Iterates over all cards in the `card_details` table, updating the normal price and inserting spot prices if necessary.
#
#     Args:
#         Session: The SQLAlchemy session factory.
#         update_normal_price (func): A function to update the normal price for a card.
#         insert_spot_price (func): A function to insert spot prices for a card if needed.
#
#     """
#     # Create a new session
#     session = Session()
#
#     try:
#         # Query all cards from the card_details table
#         card_details = session.query(CardDetails).all()
#
#         print(f"Found {len(card_details)} cards to process...")
#
#         for card in card_details:
#             try:
#                 # Step 2: Update the normal price for the card
#                 updated_card = update_normal_price(card.id)  # Assuming this function modifies the card in-place
#                 print(f"Updated normal price for card_id: {card.id}")
#
#                 # Step 3: Insert a spot price, if needed
#                 spot_price_message = insert_spot_price(Session, card, updated_card.normal_price)
#                 print(f"Spot price update result for card_id {card.id}: {spot_price_message}")
#
#             except Exception as e:
#                 print(f"Error processing card_id {card}: {e}")
#                 session.rollback()  # Rollback if there's an issue updating or inserting
#
#         print("Processing complete!")
#
#     except Exception as e:
#         print(f"Error processing cards: {e}")
#         session.rollback()  # Rollback on general errors
#
#     finally:
#         # Ensure the session is closed
#         session.close()


def find_product_by_id_or_random(product_id=None, category_id=None):
    session = Session()

    if product_id:
        # Fetch product by productId
        product = session.query(Products).filter_by(productId=product_id).first()
        if not product:
            raise ValueError(f"Product with ID {product_id} not found.")
        return product

    query = session.query(Products)

    if category_id:
        # Filter by categoryId if provided
        query = query.filter_by(categoryId=category_id)

    random_product = query.order_by(func.random()).first()
    if not random_product:
        raise ValueError("No products found.")

    return random_product


def prettify_keys(sqlalchemy_object):
    if not hasattr(sqlalchemy_object, "__dict__"):
        raise ValueError("Expected a SQLAlchemy object with a __dict__ attribute.")

    prettified_data = {}
    for key, value in sqlalchemy_object.__dict__.items():
        # Skip private keys (those starting with '_')
        if not key.startswith('_'):
            # Check if the value is valid
            if (
                    value is not None and  # Exclude None
                    not (isinstance(value, float) and math.isnan(value)) and  # Exclude float NaN
                    not (isinstance(value, str) and value.lower() == "nan")  # Exclude string "NaN"
            ):
                # Convert camelCase to 'Camel Case'
                pretty_key = re.sub(r'([a-z])([A-Z])', r'\1 \2', key).title()
                prettified_data[pretty_key] = value

    return prettified_data


def prettify_column_name(column_name):
    """
    Converts camelCase field names into human-readable format:
    e.g., 'productId' -> 'Product Id'
    """
    # Insert a space before any uppercase letter that follows a lowercase letter
    pretty_name = re.sub(r'(?<=\w)([A-Z])', r' \1', column_name)
    # Capitalize the first letter
    return pretty_name.title()


def prepare_product_data(raw_product):
    prettified = prettify_keys(raw_product)

    # Custom order for keys
    custom_order = ['productId', 'name',  'cleanName']

    ordered_data = {key: prettified.pop(key) for key in custom_order if key in prettified}

    # Add any remaining keys at the end
    ordered_data.update(prettified)
    return ordered_data







@app.route('/products/category/<category_id>', methods=['GET'])
def products_by_category(category_id):
    # Convert `category_id` to a string before using in a query
    category_str = str(category_id)

    # Query products where categoryId matches the given category_id
    products = session.query(ProductCategories).filter_by(categoryId=category_str).all()

    # Render the results on a template or return JSON (based on your needs)
    return render_template('products_by_category.html', products=products)



@app.route('/product/<product_id>', methods=['GET'])
def get_product(product_id):
    session = Session()
    product = session.query(Products).filter(Products.productId == product_id).first()

    if product:
        product_data = prettify_keys(product)
        return render_template('product_display.html', product=product_data)
    else:
        # Handle the case where there are no products in the database
        return render_template('product_display.html', product=None, error="No products available.")


@app.route('/random_product', methods=['GET'])
def random_product_view():
    session = Session()

    # Get a random product from the database
    random_product = session.query(Products).order_by(func.random()).first()

    if random_product:
        product_data = prettify_keys(random_product)

        return render_template('product_display.html', product=product_data)
    else:
        # Handle the case where there are no products in the database
        return render_template('product_display.html', product=None, error="No products available.")


@app.route('/sets/<set_code>')
def set_details(set_code):
    session = Session()
    try:
        # Fetch the set details
        set_metadata = session.query(SetDetails).filter(SetDetails.code == set_code).first()
        # If the set doesn't exist, return a 404 response
        if not set_metadata:
            return f"No set found with code '{set_code}'", 404

        # Fetch all cards from that set
        cards = session.query(CardDetails).filter(CardDetails.set == set_code).all()

        # Render the template with set details and cards
        return render_template(
            "set.html",
            set_metadata=set_metadata,
            cards=cards
        )
    finally:
        session.close()


# @app.route('/process_cards', methods=['GET'])
# def process_cards():
#     """
#     Flask route to process all cards in the card_details table.
#     """
#     try:
#         # Call the mass processing method
#         update_all_spot_prices(Session, update_normal_price, insert_spot_price)
#
#         # Respond with success
#         return "it worked!"
#
#     except Exception as e:
#         # Catch any errors during processing
#         return f"error: {{ e }}"


@app.route('/ask', methods=['GET'])
def ask():
    search_query = request.args.get('query', '', type=str)

    if not search_query:
        return render_template('ask.html', error="Please enter a search query.")

    results = session.query(CardDetails).filter(CardDetails.name.ilike(f'%{search_query}%')).all()

    return render_template('ask.html', results=results, query=search_query)


@app.route('/random', methods=['GET'])
def random_card_view():
    card_details = fetch_random_card_from_db()
    update_normal_price(card_details.id)

    if not card_details:
        return "Card not found", 404

    # Query all cards by the same artist
    cards_by_artist = session.query(CardDetails).filter(
        CardDetails.artist == card_details.artist,
        CardDetails.id != card_details.id  # Exclude the current card itself
    ).all()

    # Query other printings (same card with different versions/printings)
    other_printings = session.query(CardDetails).filter(
        CardDetails.name == card_details.name,
        CardDetails.id != card_details.id  # Exclude the current card itself
    ).all()

    return render_template(
        'card.html',
        card=card_details,
        cards_by_artist=cards_by_artist,
        other_printings=other_printings
    )


@app.route('/artists/<artist_name>')
def artist_cards(artist_name):
    session = Session()
    try:
        # Decode the artist_name parameter if it contains URL-encoded spaces (%20)
        artist_name = artist_name.replace('%20', ' ')

        # Query all card entries for this artist
        cards = session.query(CardDetails).filter(CardDetails.artist == artist_name).all()

        # If no cards for this artist are found
        if not cards:
            return f"No cards found for artist '{artist_name}'", 404

        # Render the 'artist.html' template, passing the artist name and their cards
        return render_template(
            "artist.html",
            artist_name=artist_name,
            cards=cards
        )
    finally:
        session.close()


@app.route('/gallery')
def art_gallery():
    cards = (session.query(CardDetails).filter(
            CardDetails.normal_price.isnot(None)
            ).order_by(func.random()
            ).limit(100)
             .all())

    if not cards:
        return "Error: cards not found.", 404

    return render_template(
        'gallery.html',
        cards=cards
    )


@app.route('/card/<card_id>', methods=['GET'])
def get_card(card_id):
    # Fetch the card details
    card = session.query(CardDetails).filter(CardDetails.id == card_id).first()
    update_normal_price(card_id)

    if not card:
        return "Card not found", 404

    # Query for cards by the same artist
    cards_by_artist = session.query(CardDetails).filter(
        CardDetails.artist == card.artist,  # Same artist
        CardDetails.id != card_id,  # Exclude the current card
        CardDetails.normal_price >= 0.01  # Price must be at least 0.01
    ).limit(6).all()  # Limit to 6 results

    # Query for other printings of the same card
    other_printings = session.query(CardDetails).filter(
        CardDetails.oracle_id == card.oracle_id,  # Same card identifier (e.g., oracle_id)
        CardDetails.id != card_id,  # Exclude the current card
        CardDetails.normal_price >= 0.01  # Price must be at least 0.01
    ).limit(6).all()  # Limit to 6 results

    # Access the `all_parts` JSONB field
    all_parts = card.all_parts or []  # Default to an empty list if None

    # Extract related IDs from the `all_parts` JSON, assuming it's a list of dicts
    related_ids = [part["id"] for part in all_parts if "id" in part]

    # Query related cards from the database using the extracted IDs
    related_cards = session.query(CardDetails).filter(CardDetails.id.in_(related_ids)).all()

    # Render the template with the data
    return render_template(
        'card.html',
        card=card,
        cards_by_artist=cards_by_artist,
        other_printings=other_printings,
        related_cards=related_cards
    )



@app.route('/')
def hello_world():
    try:
        hero_card = fetch_random_card_from_db()

        expensive_cards = session.query(CardDetails).filter(
            CardDetails.normal_price.isnot(None)  # Exclude rows where normal_price is NULL
        ).order_by(func.random()
        ).limit(15).all()

        random_cards = session.query(CardDetails).filter(
            CardDetails.normal_price.isnot(None),
            CardDetails.normal_price >= 0
        ).order_by(func.random()
        ).limit(15).all()

        return render_template("home.html", hero_card=hero_card, random_cards=random_cards, expensive_cards=expensive_cards)
    except Exception as e:
        print(f"Transaction failed: {e}")
        session.rollback()  # Reset the session after exception
    finally:
        session.close()  # Ensure session is closed


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
