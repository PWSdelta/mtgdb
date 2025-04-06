import math
import os
import random
import re
import shutil
import threading
import time

import requests
from datetime import datetime
import numpy as np
from flask import Flask, url_for, redirect, jsonify
from flask import request, render_template, Response, stream_with_context
from flask_caching import Cache
from flask_caching.jinja2ext import CacheExtension
from flask_cors import CORS
from flask_sitemap import Sitemap
from flask_apscheduler import APScheduler

from jinja2.ext import LoopControlExtension
from markupsafe import Markup
from sqlalchemy import create_engine, Integer, not_, or_
from sqlalchemy import inspect
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, relationship, load_only
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.sync import update
from sqlalchemy.sql import func
from sqlalchemy import text
from threading import Thread
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Also output to console
    ]
)

app = Flask(__name__)
ext = Sitemap(app)

logger = logging.getLogger(__name__)

CORS(app)



app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Configure cache
cache_config = {
    'CACHE_TYPE': 'SimpleCache',  # In production, consider 'RedisCache'
    'CACHE_DEFAULT_TIMEOUT': 86400  # 24 hours in seconds
}

cache = Cache(app, config=cache_config)


# Add the extension from flask_caching
app.jinja_env.add_extension('jinja2.ext.loopcontrols')
app.jinja_env.add_extension('jinja2.ext.do')

# app.jinja_env.globals['cached_card_image'] = cached_card_image


if os.getenv("APP_ENVIRONMENT", "").startswith("DEV"):
    engine = create_engine(os.environ.get('LOCAL_DB_URL'))
elif os.getenv("APP_ENVIRONMENT", "").startswith("DEVVV"):
    engine = create_engine(os.environ.get('RW_DATABASE_URL'))
else:
    engine = create_engine(os.environ.get('DATABASE_URL'))

# Automatically map the database schema
Base = automap_base()
Base.prepare(engine, reflect=True, schema="public")
inspector = inspect(engine)


# Access the automatically generated ORM class
CardDetails = Base.classes.card_details
Products = Base.classes.products
SpotPrices = Base.classes.spot_prices
SetDetails = Base.classes.set_details

Products.card_details = relationship(
    "card_details",
    primaryjoin="products.productId == foreign(card_details.tcgplayer_id)",
    back_populates="product"
)

CardDetails.product = relationship(
    "products",
    primaryjoin="foreign(card_details.tcgplayer_id) == products.productId",
    back_populates="card_details"
)


Session = sessionmaker(bind=engine)
session = Session()



# Create a template filter that renders and caches card images
@app.template_filter('cached_card_image')
def cached_card_image(card, timeout=86400):
    # Create a nested function that will be memoized with the specific timeout
    @cache.memoize(timeout=timeout)
    def render_card(card_id):
        # Get image_uris using safe attribute access
        image_uris = safe_get_attr(card, 'image_uris', {})
        normal_img = safe_get_attr(image_uris, 'normal', '')

        # Get other attributes safely
        card_id = safe_get_attr(card, 'id', '')
        name = safe_get_attr(card, 'name', '')
        set_name = safe_get_attr(card, 'set_name', '')
        type_line = safe_get_attr(card, 'type_line', '')
        artist = safe_get_attr(card, 'artist', '')

        # Generate slug
        slug = generate_slug(name)

        html = f'''
        <a href="/card/{card_id}/{slug}">
          <img 
            src="{normal_img}" 
            alt="{name} - {set_name} Magic: The Gathering Card - {type_line} by {artist}" 
            class="card-img-top"
            loading="lazy" >
        </a>
        '''
        return Markup(html)

    # Call the inner function with just the ID for efficient caching
    card_id = safe_get_attr(card, 'id', '')
    return render_card(card_id)



def run_task_in_background(task_func, *args, **kwargs):
    thread = Thread(target=task_func, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()
    return thread


def card_spot_price_workflow(card_id):
    with app.app_context():
        session = Session()

        try:
            print(f"Processing item {card_id}")
            card = session.query(CardDetails).filter(CardDetails.id == card_id).first()
            if card is not None:
                update_scryfall_prices(card)
                update_normal_price(card.id)
                record_daily_price(card)

        except Exception as e:
            print(f"An error occurred during the card_spot_price_workflow(): {e}")
            return None

@app.route('/spot/<card_id>')
def process_item(card_id):
    run_task_in_background(card_spot_price_workflow, card_id)
    return f"Processing started for card {card_id}"


# Helper function to safely access attributes using dot notation
def safe_get_attr(obj, attr, default=None):
    try:
        # Try attribute access first
        if hasattr(obj, attr):
            return getattr(obj, attr)
        # Try dictionary access as fallback
        elif isinstance(obj, dict) and attr in obj:
            return obj[attr]
        return default
    except:
        return default



def fetch_random_card_from_db():
    """
    Fetches a random card entry from the database.
    """
    try:
        # random_card = (session.query(CardDetails)
        #                .order_by(func.random())
        #                .first())

        # Count all cards that meet the criteria.
        total_filtered = session.query(func.count(CardDetails.id)).filter(
            CardDetails.normal_price > 0,
            CardDetails.image_uris['normal'].isnot(None)
        ).scalar()

        if total_filtered == 0:
            return None

        # Pick a random offset within the filtered range.
        random_offset = random.randint(0, total_filtered - 1)

        # Limit to one row by applying the offset.
        random_card = (session.query(CardDetails)
                       .filter(
            CardDetails.normal_price > 0,
            CardDetails.image_uris['normal'].isnot(None)
        )
                       .offset(random_offset)
                       .limit(1)
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
        # if not product:
            # raise ValueError(f"No product found with tcgplayer_id: {card.tcgplayer_id}")
            # print(f"No product found with tcgplayer_id: {card.tcgplayer_id}")

        # print(f"Product found: {product.name} | {product.productId}")

    except Exception as e:
        print(f"Error fetching card: {e}")
        return None

    # Calculate normal price
    try:
        prices = card.prices or {}
        usd_price = float(prices.get('usd', 0) or 0)
        eur_price = float(prices.get('eur', 0) or 0)
        low_price = 0.0
        mid_price = 0.0
        market_price = 0.0
        direct_low_price = 0.0

        if product:
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



def calculate_normal_price(product):
    """Calculate normalized price from various sources"""
    # Gather price sources
    prices_to_consider = []
    # Add your actual price sources here
    # Example: prices_to_consider.append(product.tcgplayer_price) if product.tcgplayer_price else None

    if not prices_to_consider:
        print(f"No valid prices found for product {product.name}")
        return None

    # Calculate with your standard method
    return float(np.mean(prices_to_consider))


def save_product_price(product, new_price):
    """Save the calculated price to database"""
    try:
        # Update database directly
        session.execute(
            update(Products)
            .where(Products.productId == product.productId)
            .values(normalPrice=new_price)
        )
        session.commit()

        # Refresh product object
        session.refresh(product)
        print(f"Updated normalPrice to {new_price} for product {product.productId}")
        return True
    except Exception as e:
        session.rollback()
        print(f"Database update failed: {e}")
        return False


def record_daily_price(card_detail):
    """
    Records the card's normal_price in the spot_prices table using SQLAlchemy UPSERT.
    One price record per card per day.

    Args:
        card_detail: The CardDetail object containing the normal_price
    """
    from datetime import datetime, time
    from sqlalchemy import func, update, insert
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    # Skip if normal_price is None or 0
    if not card_detail.normal_price:
        print(f"No normal_price available for card {card_detail.id}")
        return False

    try:
        current_time = datetime.now()

        # Create an insert statement
        insert_stmt = pg_insert(SpotPrices.__table__).values(
            card_id=card_detail.id,
            price=float(card_detail.normal_price),
            date=current_time
        )

        # Create the ON CONFLICT DO UPDATE statement
        # This checks for conflict on card__id + day
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[
                SpotPrices.card_id,
                func.date(SpotPrices.date)
            ],
            set_={
                'price': float(card_detail.normal_price),
                'date': current_time
            }
        )

        # Execute the statement
        session.execute(upsert_stmt)
        session.commit()
        print(f"Recorded spot price {card_detail.normal_price} for card {card_detail.id}")
        return True
    except Exception as e:
        session.rollback()
        print(f"Failed to record spot price: {e}")
        return False


def update_random_entities():
    """Updates prices for a random card and a random product."""
    results = {}

    try:
        # Get and update a random product
        random_product = session.query(Products).order_by(func.random()).first()
        if random_product is not None:
            update_product_price(random_product.productId)
            results['product'] = {
                'id': random_product.productId,
                'name': random_product.cleanName,
                'status': 'updated'
            }
        else:
            results['product'] = {'status': 'no products available'}

        # Get and update a random card
        random_card = session.query(CardDetails).order_by(func.random()).first()
        if random_card is not None:
            update_normal_price(random_card.id)
            results['card'] = {
                'id': random_card.id,
                'name': random_card.name,
                'status': 'updated'
            }
        else:
            results['card'] = {'status': 'no cards available'}

    except Exception as e:
        results['error'] = str(e)

    return results


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


def generate_sitemap_files():
    """Generate static sitemap files"""
    base_url = "https://pwsdelta.com"  # Replace with your actual domain
    output_dir = "static/sitemaps"  # Directory to store sitemap files

    # Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get total URLs count (replace with your actual query)
    total_cards = session.query(func.count(CardDetails.id)).scalar()

    # URLs per sitemap (reduced from 50000)
    urls_per_sitemap = 10000
    num_sitemaps = math.ceil(total_cards / urls_per_sitemap)

    # Generate sitemap index
    with open(f"{output_dir}/sitemap.xml", "w") as index_file:
        index_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        index_file.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

        for i in range(num_sitemaps):
            index_file.write('  <sitemap>\n')
            index_file.write(f'    <loc>{base_url}/static/sitemaps/sitemap-{i + 1}.xml</loc>\n')
            index_file.write(f'    <lastmod>{datetime.now().strftime("%Y-%m-%d")}</lastmod>\n')
            index_file.write('  </sitemap>\n')

        index_file.write('</sitemapindex>')

    # Generate individual sitemap files
    for sitemap_id in range(1, num_sitemaps + 1):
        print(f"Generating sitemap {sitemap_id}/{num_sitemaps}")

        with open(f"{output_dir}/sitemap-{sitemap_id}.xml", "w") as sitemap_file:
            sitemap_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            sitemap_file.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

            offset = (sitemap_id - 1) * urls_per_sitemap

            # Replace with your actual database query
            # Fetch URLs in batches to avoid memory issues
            batch_size = 1000
            for batch_offset in range(0, urls_per_sitemap, batch_size):
                # Query to get the actual card IDs
                cards = session.query(CardDetails.id).order_by(CardDetails.id).offset(offset + batch_offset).limit(
                    batch_size).all()

                # No cards left in this batch
                if not cards:
                    break

                # Use the actual card IDs from the database
                for card in cards:
                    # Get the ID value from the database query result
                    card_id = card.id if hasattr(card, 'id') else card[0]

                    # Use the actual card ID in the URL
                    url_path = f"card/{card_id}/{ card.name.lower().replace(' ', '-') }"

                    sitemap_file.write('  <url>\n')
                    sitemap_file.write(f'    <loc>{base_url}/{url_path}</loc>\n')
                    sitemap_file.write(f'    <lastmod>{datetime.now().strftime("%Y-%m-%d")}</lastmod>\n')
                    sitemap_file.write('  </url>\n')

            sitemap_file.write('</urlset>')

#
# @app.route('/asdf')
# def asdf():
#     generate_sitemap_files()
#
#     return 200


import requests


def update_scryfall_prices(card_details):
    """
    Fetches price data from Scryfall for a card and updates its prices field in the database.

    Args:
        card_details: Object containing card data with an 'id' attribute
        session: Database session for committing the changes

    Returns:
        bool: True if prices were successfully updated, False otherwise
    """
    session = Session()
    #
    # try:

    print(f"Fetching Scryfall prices for card {card_details.id}")
    # Get card data from Scryfall using the ID
    url = f"https://api.scryfall.com/cards/{card_details.id}"
    response = requests.get(url)
    response.raise_for_status()

    # Extract price data from response
    card_data = response.json()
    prices = card_data.get("prices", {})

    # Update the prices field of the card
    card_details.prices = prices

    # Commit changes to database
    session.commit()
    print(f"Successfully updated Scryfall prices for card {card_details.id}")

    return True


@app.template_filter('generate_slug')
def generate_slug(text):
    return text.lower().replace(' ', '-').replace(',', '').replace("'", '')



@app.route('/generate-sitemaps', methods=['GET'])
def generate_sitemaps():
    """Generate sitemap files for the application."""
    try:
        # Create sitemaps directory if it doesn't exist
        sitemap_dir = os.path.join(app.static_folder, 'sitemaps')
        os.makedirs(sitemap_dir, exist_ok=True)

        # Get today's date for lastmod
        today = datetime.now().strftime('%Y-%m-%d')

        # Base URL for your site
        base_url = "https://yourdomain.com"  # Replace with your actual domain

        # List to store sitemap entries for the index
        sitemap_index_entries = []

        # Generate the main sitemap for static pages
        static_sitemap_filename = "sitemap-static.xml"
        with open(os.path.join(sitemap_dir, static_sitemap_filename), 'w', encoding='utf-8') as sitemap_file:
            sitemap_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            sitemap_file.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

            # Add static pages
            static_pages = ['', 'about', 'contact', 'search', 'browse']
            for page in static_pages:
                sitemap_file.write('  <url>\n')
                sitemap_file.write(f'    <loc>{base_url}/{page}</loc>\n')
                sitemap_file.write(f'    <lastmod>{today}</lastmod>\n')
                sitemap_file.write('  </url>\n')

            sitemap_file.write('</urlset>\n')

        # Add the static sitemap to the index
        sitemap_index_entries.append(f"{base_url}/sitemaps/{static_sitemap_filename}")

        # Generate card sitemaps with batching
        batch_size = 10000  # Maximum URLs per sitemap file
        sitemap_counter = 1

        # Start with the first batch
        offset = 0

        while True:
            # Get a batch of cards using the ORM model (like in your / route)
            batch_cards = session.query(CardDetails).limit(batch_size).offset(offset).all()

            # If no more cards, break out of the loop
            if not batch_cards:
                break

            # Create a new sitemap file for this batch
            sitemap_filename = f"sitemap-cards-{sitemap_counter}.xml"
            with open(os.path.join(sitemap_dir, sitemap_filename), 'w', encoding='utf-8') as sitemap_file:
                # Write sitemap header
                sitemap_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                sitemap_file.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

                # Process each card in this batch
                cards_in_sitemap = 0
                for card in batch_cards:
                    card_id = card.id
                    card_name = card.name

                    url_path = f"/card/{card_id}/{generate_slug(card_name)}"

                    sitemap_file.write('  <url>\n')
                    sitemap_file.write(f'    <loc>{base_url}/{url_path}</loc>\n')
                    sitemap_file.write(f'    <lastmod>{today}</lastmod>\n')
                    sitemap_file.write('  </url>\n')
                    cards_in_sitemap += 1

                # Write sitemap footer
                sitemap_file.write('</urlset>\n')

                print(f"Added {cards_in_sitemap} cards to {sitemap_filename}")

            # Add this sitemap to the index
            sitemap_index_entries.append(f"{base_url}/sitemaps/{sitemap_filename}")

            # Move to the next batch
            offset += batch_size
            sitemap_counter += 1

        # Create sitemap index file
        sitemap_index_filename = "sitemap.xml"
        with open(os.path.join(app.static_folder, sitemap_index_filename), 'w', encoding='utf-8') as index_file:
            index_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            index_file.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

            for sitemap_url in sitemap_index_entries:
                index_file.write('  <sitemap>\n')
                index_file.write(f'    <loc>{sitemap_url}</loc>\n')
                index_file.write(f'    <lastmod>{today}</lastmod>\n')
                index_file.write('  </sitemap>\n')

            index_file.write('</sitemapindex>\n')

        return jsonify({
            'success': True,
            'message': f'Generated {sitemap_counter} sitemap files and index',
            'sitemap_url': f"{base_url}/sitemap.xml"
        })

    except Exception as e:
        print(f"Error generating sitemaps: {e}")
        return jsonify({
            'success': False,
            'message': f'Error generating sitemaps: {str(e)}'
        }), 500



@app.route('/sitemap.xml')
def sitemap_index():
    base_url = "https://pwsdelta.com"  # Change to your actual domain

    # Get counts
    total_cards = session.query(func.count(CardDetails.id)).scalar()
    total_products = session.query(Products).count()

    urls_per_sitemap = 10000
    num_card_sitemaps = math.ceil(total_cards / urls_per_sitemap)
    num_product_sitemaps = math.ceil(total_products / urls_per_sitemap)

    sitemap_index = '<?xml version="1.0" encoding="UTF-8"?>\n'
    sitemap_index += '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'

    # Add card sitemaps
    for i in range(num_card_sitemaps):
        sitemap_index += '  <sitemap>\n'
        sitemap_index += f'    <loc>{base_url}/sitemap-card-{i + 1}.xml</loc>\n'
        sitemap_index += f'    <lastmod>{datetime.now().strftime("%Y-%m-%d")}</lastmod>\n'
        sitemap_index += '  </sitemap>\n'

    # Add product sitemaps
    for i in range(num_product_sitemaps):
        sitemap_index += '  <sitemap>\n'
        sitemap_index += f'    <loc>{base_url}/sitemap-product-{i + 1}.xml</loc>\n'
        sitemap_index += f'    <lastmod>{datetime.now().strftime("%Y-%m-%d")}</lastmod>\n'
        sitemap_index += '  </sitemap>\n'

    sitemap_index += '</sitemapindex>'

    return Response(sitemap_index, mimetype='text/xml')



@app.route('/sitemap-card-<int:sitemap_id>.xml')
def sitemap_card(sitemap_id):
    # Serve a specific card sitemap file
    sitemap_path = f"static/sitemaps/sitemap-card-{sitemap_id}.xml"

    if os.path.exists(sitemap_path):
        with open(sitemap_path, 'r') as f:
            sitemap_content = f.read()
        return Response(sitemap_content, mimetype='text/xml')
    else:
        return "Sitemap not found", 404


@app.route('/sitemap-product-<int:sitemap_id>.xml')
def sitemap_product(sitemap_id):
    # Serve a specific product sitemap file
    sitemap_path = f"static/sitemaps/sitemap-product-{sitemap_id}.xml"

    if os.path.exists(sitemap_path):
        with open(sitemap_path, 'r') as f:
            sitemap_content = f.read()
        return Response(sitemap_content, mimetype='text/xml')
    else:
        return "Sitemap not found", 404



# Keep the original route for backward compatibility
@app.route('/sitemap-<int:sitemap_id>.xml')
def sitemap(sitemap_id):
    # This will serve the existing backward-compatible file
    sitemap_path = f"static/sitemaps/sitemap-{sitemap_id}.xml"

    if os.path.exists(sitemap_path):
        with open(sitemap_path, 'r') as f:
            sitemap_content = f.read()
        return Response(sitemap_content, mimetype='text/xml')
    else:
        return "Sitemap not found", 404




# Flask routes with SEO-friendly URLs
@app.route('/product/<product_slug>/', methods=['GET'])
def product_detail(product_slug):
    product = session.query(Products).filter_by(slug=product_slug).first_or_404()
    return render_template('product_detail.html', product=product)


@app.route('/product/<product_id>', methods=['GET'])
def get_product(product_id):
    session = Session()
    product = session.query(Products).filter(Products.productId == product_id).first()

    if product:
        product_data = prettify_keys(product)
        update_product_price(product_id)
        return render_template('product.html', product=product_data)
    else:
        # Handle the case where there are no products in the database
        return render_template('product.html', product=None, error="No products available.")


@app.route('/random_product', methods=['GET'])
def random_product_view():
    session = Session()

    # Get a random product from the database
    random_product = session.query(Products).order_by(func.random()).first()

    if random_product:
        update_product_price(random_product.productId)
        product_data = prettify_keys(random_product)

        return render_template('product.html', product=product_data)
    else:
        # Handle the case where there are no products in the database
        return render_template('product.html', product=None, error="No products available.")


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


@app.route('/ask', methods=['GET'])
def ask():
    search_query = request.args.get('query', '', type=str)

    if not search_query:
        return render_template('search.html', error="Please enter a search query.")

    results = session.query(CardDetails).filter(CardDetails.name.ilike(f'%{search_query}%')).all()

    return render_template('search.html', results=results, query=search_query)



@app.route('/search')
def search():
    # If there are no search parameters, just render the form
    if not any(request.args.values()):
        return render_template('search.html')

    # Get all search parameters from URL
    card_name = request.args.get('name', '')
    card_text = request.args.get('text', '')
    card_type = request.args.get('type', '')
    colors = request.args.getlist('colors')
    colors_str = ''.join(colors)
    color_match = request.args.get('colorMatch', 'exact')
    cmc_min = request.args.get('manaMin', '')
    cmc_max = request.args.get('manaMax', '')
    rarity = request.args.getlist('rarity')
    card_set = request.args.get('set', '')
    mtg_format = request.args.get('format', '')
    power_min = request.args.get('powerMin', '')
    power_max = request.args.get('powerMax', '')
    toughness_min = request.args.get('toughnessMin', '')
    toughness_max = request.args.get('toughnessMax', '')

    # Start a session
    session = Session()

    # Build the query using SQLAlchemy's query API
    query = session.query(CardDetails)

    # Apply filters based on parameters
    if card_name:
        query = query.filter(CardDetails.name.ilike(f'%{card_name}%'))

    if card_text:
        query = query.filter(CardDetails.oracle_text.ilike(f'%{card_text}%'))

    if card_type:
        types = card_type.split(',')
        type_filters = [CardDetails.type_line.ilike(f'%{t}%') for t in types]
        query = query.filter(or_(*type_filters))

    if colors_str:
        # Handle color matching based on color_match parameter
        if color_match == 'exact':
            # Exact color match (no more, no less)
            query = query.filter(CardDetails.colors == colors_str)
        elif color_match == 'includes':
            # Must include all specified colors (may have more)
            for color in colors_str:
                query = query.filter(CardDetails.colors.ilike(f'%{color}%'))
        elif color_match == 'at-most':
            # Only the specified colors, but not necessarily all of them
            for color in 'WUBRG':
                if color not in colors_str:
                    query = query.filter(not_(CardDetails.colors.ilike(f'%{color}%')))

    if cmc_min:
        query = query.filter(CardDetails.cmc >= float(cmc_min))

    if cmc_max:
        query = query.filter(CardDetails.cmc <= float(cmc_max))

    if rarity:
        query = query.filter(CardDetails.rarity.in_(rarity))

    if card_set:
        sets = card_set.split(',')
        query = query.filter(CardDetails.set_code.in_(sets))

    if mtg_format:
        # This depends on how you store format legality in your database
        # For example, if you have a column named standard_legal
        format_column = getattr(CardDetails, f"{mtg_format}_legal")
        query = query.filter(format_column == True)

    # Handle power/toughness for creatures
    # Note: Since power/toughness can be non-numeric (like '*'), we need to be careful
    if power_min:
        # Filter only numeric power values greater than or equal to power_min
        query = query.filter(CardDetails.power.op('REGEXP')('^[0-9]+$'))
        query = query.filter(func.cast(CardDetails.power, Integer) >= int(power_min))

    if power_max:
        query = query.filter(CardDetails.power.op('REGEXP')('^[0-9]+$'))
        query = query.filter(func.cast(CardDetails.power, Integer) <= int(power_max))

    if toughness_min:
        query = query.filter(CardDetails.toughness.op('REGEXP')('^[0-9]+$'))
        query = query.filter(func.cast(CardDetails.toughness, Integer) >= int(toughness_min))

    if toughness_max:
        query = query.filter(CardDetails.toughness.op('REGEXP')('^[0-9]+$'))
        query = query.filter(func.cast(CardDetails.toughness, Integer) <= int(toughness_max))

    # Count total results for pagination before applying limits
    total_cards = query.count()

    # Add pagination
    page = request.args.get('page', 1, type=int)
    per_page = 30  # Cards per page
    cards = query.order_by(CardDetails.name).offset((page - 1) * per_page).limit(per_page).all()

    total_pages = (total_cards + per_page - 1) // per_page

    # Close the session
    session.close()

    # Render the template with results and search parameters
    return render_template(
        'search.html',
        cards=cards,
        total_cards=total_cards,
        page=page,
        total_pages=total_pages,
        search_params=request.args,
        url_query_string=request.query_string.decode()
    )


@app.route('/random', methods=['GET'])
def random_card_view():
    card_details = fetch_random_card_from_db()

    update_scryfall_prices(card_details)
    update_normal_price(card_details.id)
    record_daily_price(card_details)

    if not card_details:
        return "Card not found", 404

    cards_by_artist = session.query(CardDetails).filter(
        CardDetails.artist == card_details.artist,  # Same artist
        CardDetails.id != card_details.id,  # Exclude the current card
        CardDetails.normal_price >= 0.01  # Price must be at least 0.01
    ).limit(9).all()  # Limit to 6 results

    # Query other printings (same card with different versions/printings)
    other_printings = session.query(CardDetails).filter(
        CardDetails.name == card_details.name,
        CardDetails.id != card_details.id  # Exclude the current card itself
    ).limit(9).all()

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
        cards = session.query(CardDetails).filter(CardDetails.artist == artist_name).limit(999).all()

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
            ).limit(300)
             .all())

    if not cards:
        return "Error: cards not found.", 404

    return render_template(
        'gallery.html',
        cards=cards
    )


@app.route('/card/<card_id>', methods=['GET'])
def card_legacy(card_id):
    # Get card data
    card = session.query(CardDetails).filter(CardDetails.id == card_id).first()

    if card:
        # Generate the slug
        slug = generate_slug(card.name)
        # Redirect to new URL format with 301 (permanent) redirect
        return redirect(url_for('card_detail', card_id=card_id, card_slug=slug), code=301)
    return render_template('404.html'), 404


@app.route('/card/<card_id>/<card_slug>')
def card_detail(card_id, card_slug):
    # Fetch the card details
    card = session.query(CardDetails).filter(CardDetails.id == card_id).first()

    if card is not None:
        update_scryfall_prices(card)
        update_normal_price(card.id)
        record_daily_price(card)

    if not card:
        return "Card not found", 404

    # Query for other printings of the same card
    other_printings = session.query(
            CardDetails.id,
            CardDetails.name,
            CardDetails.artist,
            CardDetails.oracle_text,
            CardDetails.printed_text,
            CardDetails.flavor_text,
            CardDetails.set_name,
            CardDetails.tcgplayer_id,
            CardDetails.normal_price,
            CardDetails.image_uris["normal"].label("normal_image")
        ).filter(
        CardDetails.oracle_id == card.oracle_id,  # Same card identifier (e.g., oracle_id)
        CardDetails.id != card_id,  # Exclude the current card
        CardDetails.normal_price >= 0.01  # Price must be at least 0.01
    ).limit(9999).all()  # Limit to 6 results

    # Query for cards by the same artist
    cards_by_artist = session.query(
            CardDetails.id,
            CardDetails.name,
            CardDetails.artist,
            CardDetails.oracle_text,
            CardDetails.printed_text,
            CardDetails.flavor_text,
            CardDetails.set_name,
            CardDetails.tcgplayer_id,
            CardDetails.normal_price,
            CardDetails.image_uris["normal"].label("normal_image")
        ).filter(
        CardDetails.artist == card.artist,  # Same artist
        CardDetails.id != card_id,  # Exclude the current card
        CardDetails.normal_price >= 0.01  # Price must be at least 0.01
    ).limit(9999).all()  # Limit to 6 results

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


@app.route('/', methods=['GET', 'HEAD'])
def hello_world():
    try:
        hero_card = fetch_random_card_from_db()

        random_cards = session.query(
            CardDetails.id,
            CardDetails.name,
            CardDetails.artist,
            CardDetails.oracle_text,
            CardDetails.printed_text,
            CardDetails.flavor_text,
            CardDetails.set_name,
            CardDetails.tcgplayer_id,
            CardDetails.normal_price,
            CardDetails.image_uris["normal"].label("normal_image")
        ).filter(
    CardDetails.normal_price > 0,
            CardDetails.normal_price.isnot(None),
            CardDetails.image_uris["normal"].isnot(None)
        ).order_by(func.random()).limit(37).all() or []

        # Render the page with whatever data we have
        return render_template(
            "home.html",
            hero_card=hero_card,
            random_cards=random_cards
        )
    except Exception as e:
        print(f"Transaction failed: {str(e)}")
        session.rollback()
        return render_template("error.html", error=str(e)), 500
    finally:
        session.close()



@app.route('/robots.txt')
def robots():
    return Response("""
        User-agent: *
        Allow: /
        Sitemap: https://pwsdelta.com/sitemap.xml
        """, mimetype='text/plain')


#
@app.route('/asdf')
def asdf():
    session = Session()

    card_ids = session.query(CardDetails.id).all()

    for card in card_ids:
        current_card = session.query(CardDetails).filter(CardDetails.id == card.id).first()
        update_normal_price(current_card.id)
        record_daily_price(current_card)

        time.sleep(random.uniform(1.31, 3.77))

    render_template("home.html", message="Sitemap generation complete")


if __name__ == '__main__':
    app.run()

