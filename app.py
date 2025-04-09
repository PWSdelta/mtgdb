import json
import logging
import math
import os
import random
import re
from datetime import datetime
from threading import Thread

import numpy as np
from flask import Flask, url_for, redirect, jsonify, abort, flash
from flask import request, render_template, Response
from flask_caching import Cache
from flask_cors import CORS
from flask_sitemap import Sitemap
from markupsafe import Markup
from psycopg2.extensions import JSON, JSONB
from sqlalchemy import create_engine, Integer, not_, or_, desc, MetaData, Table, Column, String, text
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

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
app.secret_key = 'B87A0C9SQ54HBT3WBL-0998A3VNM09287NV0'

1
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
# Decks = Base.classes.decks
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

@cache.cached(timeout=300, key_prefix=lambda: f"cards_by_artist_{request.view_args['card_id']}")
def get_cards_by_artist(card, card_id):
    return session.query(
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
        CardDetails.artist == card.artist,
        CardDetails.id != card_id,
        CardDetails.normal_price >= 0.01
    ).limit(9999).all()


@cache.cached(timeout=300, key_prefix=lambda: f"other_printings_{request.view_args['card_id']}")
def get_other_printings(card, card_id):
    return session.query(
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
        CardDetails.oracle_id == card.oracle_id,
        CardDetails.id != card_id,
        CardDetails.normal_price >= 0.01
    ).limit(9999).all()



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
            # card = session.query(CardDetails).filter(CardDetails.id == card_id).first()
            # if card is not None:
                # update_scryfall_prices(card)
                # update_normal_price(card.id)
                # record_daily_price(card)

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

        product = None
        if card.tcgplayer_id:  # Only try to find product if tcgplayer_id exists
            product = session.query(Products).filter(Products.productId == card.tcgplayer_id).first()
            if product:
                print(f"Product found: {product.name} | {product.productId}")
            else:
                print(f"No product found with tcgplayer_id: {card.tcgplayer_id}")

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

        # If we don't have product prices but have at least one of usd or eur, use those
        if not product and (usd_price > 0 or eur_price > 0):
            valid_prices = [price for price in [usd_price, eur_price] if price > 0]
            print(f"Using only card prices (no product): {valid_prices}")

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


def record_daily_price(card_detail):
    """
    Records the card's normal_price in the spot_prices table using SQLAlchemy UPSERT.
    One price record per card per day.

    Args:
        card_detail: The CardDetail object containing the normal_price
    """
    from datetime import datetime
    from sqlalchemy import func
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




@app.route('/sets/<set_code>')
def set_details(set_code):
    session = Session()
    try:
        # Using the correct field name 'set' instead of 'set_name'
        print(f"Searching for cards with set = {set_code}")

        # Query using the 'set' attribute
        cards_in_set = session.query(CardDetails).filter(CardDetails.set == set_code).all()

        print(f"Found {len(cards_in_set)} cards for set = {set_code}")

        if not cards_in_set:
            # Check what set values exist in the database
            sample_sets = session.query(CardDetails.set).distinct().limit(10).all()
            print(f"Sample set codes in database: {[s[0] for s in sample_sets]}")

            # Check if there's a close match (in case of capitalization differences)
            close_matches = session.query(CardDetails.set).filter(
                CardDetails.set.ilike(f"%{set_code}%")
            ).distinct().limit(5).all()

            print(f"Possible close matches: {[s[0] for s in close_matches]}")

            return f"Set '{set_code}' not found. Possible sets: {', '.join([s[0] for s in sample_sets])}", 404

        # Pick a random card from the set
        import random
        card = random.choice(cards_in_set)

        # Debug: Print which card we're processing
        print(f"Selected card: {card.name if hasattr(card, 'name') else card.id}")

        # Execute the full 3-method spot price update workflow.
        # update_scryfall_prices(card)
        # update_normal_price(card.id)
        # record_daily_price(card)

        return render_template('set.html', card=card, set_code=set_code, cards=cards_in_set)

    except Exception as e:
        import traceback
        print(f"Error in set_details: {e}")
        print(traceback.format_exc())
        return f"An error occurred: {e}", 500
    finally:
        session.close()



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


@app.route('/artists/<artist_name>')
def artist_cards(artist_name):
    session = Session()
    try:
        # Decode the artist_name parameter if it contains URL-encoded spaces (%20)
        artist_name = artist_name.replace('%20', ' ')

        # Query all cards for this artist
        cards = (session.query(CardDetails)
                 .filter(CardDetails.artist == artist_name)
                 .order_by(CardDetails.normal_price.desc())
                 .limit(999).all())

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
    if not card:
        return "Card not found", 404

    # update_scryfall_prices(card)
    # update_normal_price(card.id)
    # record_daily_price(card)

    cards_by_artist = get_cards_by_artist(card, card_id)
    other_printings = get_other_printings(card, card_id)

    # Render the template with the data
    return render_template(
        'card.html',
        card=card,
        cards_by_artist=cards_by_artist,
        other_printings=other_printings
    )

    # Query for other printings of the same card
    # other_printings = session.query(
    #         CardDetails.id,
    #         CardDetails.name,
    #         CardDetails.artist,
    #         CardDetails.oracle_text,
    #         CardDetails.printed_text,
    #         CardDetails.flavor_text,
    #         CardDetails.set_name,
    #         CardDetails.tcgplayer_id,
    #         CardDetails.normal_price,
    #         CardDetails.image_uris["normal"].label("normal_image")
    #     ).filter(
    #     CardDetails.oracle_id == card.oracle_id,  # Same card identifier (e.g., oracle_id)
    #     CardDetails.id != card_id,  # Exclude the current card
    #     CardDetails.normal_price >= 0.01  # Price must be at least 0.01
    # ).limit(9999).all()  # Limit to 6 results
    #
    # # Query for cards by the same artist
    # cards_by_artist = session.query(
    #         CardDetails.id,
    #         CardDetails.name,
    #         CardDetails.artist,
    #         CardDetails.oracle_text,
    #         CardDetails.printed_text,
    #         CardDetails.flavor_text,
    #         CardDetails.set_name,
    #         CardDetails.tcgplayer_id,
    #         CardDetails.normal_price,
    #         CardDetails.image_uris["normal"].label("normal_image")
    #     ).filter(
    #     CardDetails.artist == card.artist,  # Same artist
    #     CardDetails.id != card_id,  # Exclude the current card
    #     CardDetails.normal_price >= 0.01  # Price must be at least 0.01
    # ).limit(9999).all()  # Limit to 6 results





@app.route('/')
def index():
    session = Session()
    try:
        # Try to get the cached hero card ID
        hero_card_id = cache.get('hero_card_id')
        hero_card = None

        if hero_card_id is not None:
            # Get the full hero card by ID
            hero_card = session.query(CardDetails).get(hero_card_id)

        if hero_card is None:
            # If not in cache or not found, get a random card
            hero_card = fetch_random_card_from_db()
            if hero_card:
                # Cache just the ID for 10 minutes
                cache.set('hero_card_id', hero_card.id, timeout=600)

        # Now handle the random cards with proper cache implementation
        random_card_ids = cache.get('random_card_ids')
        random_cards = []

        if random_card_ids is not None and random_card_ids:  # Make sure it's not None and not empty
            # Retrieve cards using cached IDs
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
                CardDetails.id.in_(random_card_ids)
            ).all() or []  # Use empty list if None

            if random_cards:
                # Sort them to match the original random order
                id_to_position = {id: i for i, id in enumerate(random_card_ids)}
                random_cards.sort(key=lambda card: id_to_position.get(card.id, 0))

        # If we didn't get cards from cache, generate new ones
        if not random_cards:
            random_cards = get_randomized_top_cards(session) or []

        return render_template('home.html', hero_card=hero_card, random_cards=random_cards)

    except Exception as e:
        import traceback
        print(f"Error in index route: {e}")
        print(traceback.format_exc())
        return f"An error occurred: {e}", 500
    finally:
        session.close()




def get_randomized_top_cards(session):
    """
    Get 300 randomized cards from the top 3000 highest-priced cards.
    With improved debugging and flexible criteria.
    """
    # First, let's check how many cards meet our criteria at all
    count = session.query(CardDetails).filter(
        CardDetails.normal_price.isnot(None),
        CardDetails.normal_price > 0
    ).count()

    print(f"DEBUG: Found {count} cards with normal_price > 0 and not null")

    # If we have very few, we should adjust our strategy
    if count < 100:
        print("WARNING: Very few cards meet price criteria, relaxing constraints")
        # Try without the price filter if we have too few cards
        top_cards = session.query(
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
            CardDetails.image_uris["normal"].isnot(None)
        ).order_by(
            desc(CardDetails.normal_price)
        ).limit(3000).all()
    else:
        # Original query with normal price criteria
        top_cards = session.query(
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
        ).order_by(
            desc(CardDetails.normal_price)
        ).limit(3000).all()

    print(f"DEBUG: Query returned {len(top_cards)} cards")

    # Check if we got any results
    if not top_cards:
        print("ERROR: No cards matched the query criteria in get_randomized_top_cards")
        return []

    # Step 2: Randomize the results
    random.shuffle(top_cards)

    # Step 3: Take the first 300 (or fewer if we have less than 300)
    limit = min(300, len(top_cards))
    random_selection = top_cards[:limit]

    print(f"DEBUG: Returning {len(random_selection)} randomized cards")

    # Cache the IDs for future use
    if random_selection:
        random_card_ids = [card.id for card in random_selection]
        cache.set('random_card_ids', random_card_ids, timeout=600)  # 10 minutes

    return random_selection


@app.route('/random-expensive')
def random_expensive_cards():
    session = Session()
    try:
        # Try to get the cached random card IDs
        random_card_ids = cache.get('random_card_ids')

        if random_card_ids is not None:
            # If we have cached IDs, retrieve those specific cards
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
                CardDetails.id.in_(random_card_ids)
            ).all()

            # Sort them to match the original random order
            id_to_position = {id: i for i, id in enumerate(random_card_ids)}
            random_cards.sort(key=lambda card: id_to_position.get(card.id, 0))
        else:
            # If not in cache, generate new random selection
            random_cards = get_randomized_top_cards()

        return render_template('random_cards.html', cards=random_cards)

    except Exception as e:
        import traceback
        print(f"Error in random_expensive_cards: {e}")
        print(traceback.format_exc())
        return f"An error occurred: {e}", 500
    finally:
        session.close()


@app.route('/generate_sitemaps')
def generate_sitemaps():
    """
    Generates sitemap files for CardDetails, splitting them into multiple files
    if the number of entries exceeds the maximum allowed per sitemap.
    """
    db_session = Session()
    base_url = "https://pwsdelta.com"  # Replace with your actual domain

    # Use the correct static folder
    sitemap_dir = os.path.join(app.static_folder, 'sitemaps')
    os.makedirs(sitemap_dir, exist_ok=True)

    today = datetime.now().strftime('%Y-%m-%d')
    urls_per_sitemap = 9999  # Limit to avoid exceeding sitemap size limits
    total_cards = 0
    sitemap_index_entries = []

    logger.info("Starting sitemap generation...")

    try:
        logger.info("Querying total number of cards...")
        total_cards = db_session.query(func.count(CardDetails.id)).scalar()
        logger.info(f"Total cards found: {total_cards}")

        num_sitemaps = math.ceil(total_cards / urls_per_sitemap)
        logger.info(f"Number of sitemaps to generate: {num_sitemaps}")

        for sitemap_id in range(1, num_sitemaps + 1):
            sitemap_filename = f"sitemap-cards-{sitemap_id}.xml"
            sitemap_path = os.path.join(sitemap_dir, sitemap_filename)

            logger.info(f"Generating sitemap file: {sitemap_filename}")

            with open(sitemap_path, 'w', encoding='utf-8') as sitemap_file:
                sitemap_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                sitemap_file.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

                offset = (sitemap_id - 1) * urls_per_sitemap
                logger.info(f"Querying cards with offset: {offset}, limit: {urls_per_sitemap}")
                cards = db_session.query(CardDetails).order_by(CardDetails.id).offset(offset).limit(
                    urls_per_sitemap).all()
                logger.info(f"Number of cards in this sitemap: {len(cards)}")

                for card in cards:
                    slug = generate_slug(card.name)
                    url = f"{base_url}/card/{card.id}/{slug}"  # Adjust URL structure if needed

                    sitemap_file.write('  <url>\n')
                    sitemap_file.write(f'    <loc>{url}</loc>\n')
                    sitemap_file.write(f'    <lastmod>{today}</lastmod>\n')
                    sitemap_file.write('  </url>\n')

                sitemap_file.write('</urlset>\n')
            sitemap_index_entries.append(f"{base_url}/sitemaps/{sitemap_filename}")
            logger.info(f"Sitemap file generated: {sitemap_filename}")

        # Create sitemap index file in the static folder
        sitemap_index_filename = "sitemap.xml"
        sitemap_index_path = os.path.join(app.static_folder, sitemap_index_filename)  # Correct path

        logger.info(f"Generating sitemap index file: {sitemap_index_filename} in {app.static_folder}")

        with open(sitemap_index_path, 'w', encoding='utf-8') as index_file:
            index_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            index_file.write('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

            for sitemap_url in sitemap_index_entries:
                index_file.write('  <sitemap>\n')
                index_file.write(f'    <loc>{sitemap_url}</loc>\n')
                index_file.write(f'    <lastmod>{today}</lastmod>\n')
                index_file.write('  </sitemap>\n')

            index_file.write('</sitemapindex>\n')

        logger.info("Sitemap index file generated: sitemap.xml")

        return jsonify({'success': True, 'message': 'Sitemaps generated successfully',
                        'sitemap_index': f"{base_url}/{sitemap_index_filename}"})

    except Exception as e:
        logger.error(f"Error generating sitemaps: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'Error generating sitemaps: {str(e)}'}), 500
    finally:
        db_session.close()
        logger.info("Sitemap generation completed.")


@app.route('/sitemap.xml')
def sitemap_index():
    """
    Serves the static sitemap index file.
    """
    return app.send_static_file('sitemap.xml')


@app.route('/sitemaps/<filename>')
def serve_sitemap(filename):
    """
    Serves individual sitemap files from the static/sitemaps directory.
    """
    return app.send_static_file(os.path.join('sitemaps', filename))


@app.route('/robots.txt')
def robots():
    return Response("""
User-agent: *
Allow: /
Sitemap: https://pwsdelta.com/sitemap.xml

# Block specific bots
User-agent: AhrefsBot
Disallow: /

User-agent: SemrushBot
Disallow: /

User-agent: MJ12bot
Disallow: /

User-agent: DotBot
Disallow: /

User-agent: Baiduspider
Disallow: /

User-agent: ClaudeBot
Disallow: /

# Block all bots from specific paths
User-agent: *
Disallow: /admin/
Disallow: /private/
Disallow: /api/
""", mimetype='text/plain')



# @app.route('/wp-admin/setup-config.php', methods=['GET', 'POST'])
# def fake_wordpress_setup():
#     # Log the attempt
#     ip_address = request.remote_addr
#     user_agent = request.headers.get('User-Agent', 'Unknown')
#
#     print(f"WordPress probe attempt detected from IP: {ip_address}, User-Agent: {user_agent}")
#
#     # Optional: Log to database or file
#     with open('intrusion_attempts.log', 'a') as f:
#         f.write(f"{datetime.now()}, {ip_address}, {user_agent}, wp-admin/setup-config.php\n")
#
#     return 404



# @app.route('/asdf', methods=['GET'])
# def asdf():

@app.route('/deck/<int:deck_id>')
def view_deck(deck_id):
    """
    Display a single deck and all its card_details.
    """
    try:
        # Get the deck by ID, using our now-working approach
        with engine.connect() as connection:
            deck_result = connection.execute(
                text("SELECT id, filename, document FROM decks WHERE id = :deck_id"),
                {"deck_id": deck_id}
            ).fetchone()

            if not deck_result:
                flash(f"Deck with ID {deck_id} not found.", "warning")
                return redirect(url_for('index'))

        # Access columns by index since we know this works
        deck_id_value = deck_result[0]
        filename_value = deck_result[1]
        document_value = deck_result[2]  # This should be a dict based on our test

        # For debugging
        print(f"Processing deck ID: {deck_id_value}")
        print(f"Document keys: {document_value.keys() if isinstance(document_value, dict) else 'Not a dict'}")

        # Create the final deck dictionary with database fields
        final_deck = {
            'id': deck_id_value,
            'filename': filename_value
        }

        # Copy over relevant fields from the document
        # First determine the main data source - document or document['data']
        deck_data = document_value
        if isinstance(document_value, dict) and 'data' in document_value:
            deck_data = document_value['data']
            print("Using 'data' field from document")

        # Add all available fields from deck_data
        if isinstance(deck_data, dict):
            for key, value in deck_data.items():
                final_deck[key] = value
            print(f"Added fields from deck_data: {list(deck_data.keys())}")

        # Now collect all cards
        all_cards = []

        # Add commander cards if present
        if isinstance(deck_data, dict) and 'commander' in deck_data and deck_data['commander']:
            commanders = deck_data['commander']
            if isinstance(commanders, list):
                for card in commanders:
                    if isinstance(card, dict):
                        card_with_flag = card.copy()  # Make a copy to avoid modifying original
                        card_with_flag['is_commander'] = True
                        all_cards.append(card_with_flag)
                print(f"Added {len(commanders)} commander cards")

        # Add regular cards if present
        if isinstance(deck_data, dict) and 'cards' in deck_data and deck_data['cards']:
            cards = deck_data['cards']
            if isinstance(cards, list):
                for card in cards:
                    if isinstance(card, dict):
                        card_with_flag = card.copy()  # Make a copy to avoid modifying original
                        card_with_flag['is_commander'] = False
                        all_cards.append(card_with_flag)
                print(f"Added {len(cards)} regular cards")

        print(f"Total cards to render: {len(all_cards)}")

        # For debugging, print a sample card if available
        if all_cards:
            print(f"Sample card keys: {list(all_cards[0].keys())}")

        # Render the template with our data
        return render_template('deck.html', deck=final_deck, cards=all_cards)

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error when viewing deck {deck_id}:\n{error_details}")
        flash(f"An error occurred: {str(e)}", "danger")
        return redirect(url_for('index'))



@app.route('/decks')
def list_decks():
    """List all available decks, limited to 31"""
    try:
        with engine.connect() as connection:
            # Fetch decks with a limit of 31
            result = connection.execute(text("SELECT id, filename, document FROM decks ORDER BY id LIMIT 31"))

            # Convert result rows to dictionaries properly
            decks = []
            for row in result:
                # Use direct index access which we know works
                deck_id = row[0]
                filename = row[1]
                document = row[2]

                # Create basic deck info
                deck_info = {
                    'id': deck_id,
                    'filename': filename
                }

                # Extract name and other metadata from document if available
                if isinstance(document, dict):
                    deck_data = document.get('data', document)  # Try to get 'data' or use document itself

                    if isinstance(deck_data, dict):
                        # Add name and other important fields
                        deck_info['name'] = deck_data.get('name', 'Unnamed Deck')
                        deck_info['type'] = deck_data.get('type', 'Unknown')

                        # Count cards if available
                        card_count = 0
                        if 'cards' in deck_data and isinstance(deck_data['cards'], list):
                            card_count += len(deck_data['cards'])
                        if 'commander' in deck_data and isinstance(deck_data['commander'], list):
                            card_count += len(deck_data['commander'])
                        deck_info['card_count'] = card_count

                decks.append(deck_info)

        # Now render the template with our list of decks
        return render_template('deck_list.html', decks=decks)

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()

        # Display the error instead of redirecting
        return f"""
        <h1>Error in Decks Route</h1>
        <p>Error: {str(e)}</p>
        <pre>{error_details}</pre>
        <p><a href="/">Return to Home</a></p>
        """, 500




if __name__ == '__main__':
    app.run()

