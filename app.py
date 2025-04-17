import json
import logging
import math
import os
import threading
import time
import traceback
from datetime import datetime, timezone

import requests
from bson import ObjectId, json_util
from flask import Flask, jsonify, request
from flask import render_template, Response
from flask_caching import Cache
from flask_cors import CORS
from flask_sitemap import Sitemap
from pymongo import MongoClient

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

# Determine environment
ENVIRONMENT = os.environ.get('FLASK_ENV', 'development')
logger = logging.getLogger(__name__)
CORS(app)



# Connect to MongoDB running on localhost with default port 27017
# client = MongoClient(os.getenv("MONGO_URI"))
client = MongoClient(os.getenv("MONGO_URI"))

# Access a database
db = client['mtgdbmongo']  # Replace with your actual database name

# Access a collection
collection = db['cards']  # Replace with your actual collection name




app.config['TEMPLATES_AUTO_RELOAD'] = True
app.secret_key = 'B87A0C9SQ54HBT3WBL-0998A3VNM09287NV0'


# Configure cache based on environment
if ENVIRONMENT == 'development':
    cache_config = {
        'CACHE_TYPE': 'SimpleCache',
        'CACHE_DEFAULT_TIMEOUT': 600
    }
else:
    # Minimal Redis config for testing in non-dev environments
    cache_config = {
        'CACHE_TYPE': 'RedisCache',
        'CACHE_REDIS_HOST': os.environ.get('REDIS_HOST', 'localhost'),
        'CACHE_REDIS_PORT': int(os.environ.get('REDIS_PORT', 6379)),
        'CACHE_DEFAULT_TIMEOUT': 600,
        'CACHE_KEY_PREFIX': 'pwsdelta_'  # Prefix all cache keys
    }

# Initialize cache
cache = Cache(app, config=cache_config)



# Add the extension from flask_caching
app.jinja_env.add_extension('jinja2.ext.loopcontrols')
app.jinja_env.add_extension('jinja2.ext.do')

# app.jinja_env.globals['cached_card_image'] = cached_card_image





class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

# Then in your Flask app:
app.json_encoder = JSONEncoder

def convert_mongo_doc(doc):
    """Convert MongoDB document to be JSON serializable"""
    if isinstance(doc, list):
        return [convert_mongo_doc(item) for item in doc]

    if isinstance(doc, dict):
        return {k: convert_mongo_doc(v) for k, v in doc.items()}

    if isinstance(doc, ObjectId):
        return str(doc)

    return doc


def fetch_random_card_from_db():
    try:
        # Initialize MongoDB connection
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']

        # Find a random card with an image
        pipeline = [
            {"$match": {
                "image_uris.normal": {"$exists": True},
                "tcgplayer_id": {"$ne": None},
                "name": {"$exists": True}
            }},
            {"$sample": {"size": 1}}
        ]

        random_card = list(cards_collection.aggregate(pipeline))

        # Check if we found a card
        if random_card and len(random_card) > 0:
            return random_card[0]

        # Fallback: just get any random card if the pipeline didn't work
        random_card = cards_collection.find_one({"name": {"$exists": True}})
        return random_card
    except Exception as e:
        print(f"An error occurred while fetching a random card: {str(e)}")
        # Return a default card if something goes wrong
        return {
            "id": "default",
            "name": "Magic Card",
            "image_uris": {
                "normal": "/static/images/card-back.jpg"
            }
        }
    finally:
        if 'client' in locals():
            client.close()


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


@app.template_filter('generate_slug')
def generate_slug(text):
    return text.lower().replace(' ', '-').replace(',', '').replace("'", '')

# @app.route('/search')
# def search():
#     # If there are no search parameters, just render the form
#     if not any(request.args.values()):
#         return render_template('search.html')
#
#     # Get all search parameters from URL
#     card_name = request.args.get('name', '')
#     card_text = request.args.get('text', '')
#     card_type = request.args.get('type', '')
#     colors = request.args.getlist('colors')
#     colors_str = ''.join(colors)
#     color_match = request.args.get('colorMatch', 'exact')
#     cmc_min = request.args.get('manaMin', '')
#     cmc_max = request.args.get('manaMax', '')
#     rarity = request.args.getlist('rarity')
#     card_set = request.args.get('set', '')
#     mtg_format = request.args.get('format', '')
#     power_min = request.args.get('powerMin', '')
#     power_max = request.args.get('powerMax', '')
#     toughness_min = request.args.get('toughnessMin', '')
#     toughness_max = request.args.get('toughnessMax', '')
#
#     # Start a session
#     session = Session()
#
#     # Build the query using SQLAlchemy's query API
#     query = session.query(CardDetails)
#
#     # Apply filters based on parameters
#     if card_name:
#         query = query.filter(CardDetails.name.ilike(f'%{card_name}%'))
#
#     if card_text:
#         query = query.filter(CardDetails.oracle_text.ilike(f'%{card_text}%'))
#
#     if card_type:
#         types = card_type.split(',')
#         type_filters = [CardDetails.type_line.ilike(f'%{t}%') for t in types]
#         query = query.filter(or_(*type_filters))
#
#     if colors_str:
#         # Handle color matching based on color_match parameter
#         if color_match == 'exact':
#             # Exact color match (no more, no less)
#             query = query.filter(CardDetails.colors == colors_str)
#         elif color_match == 'includes':
#             # Must include all specified colors (may have more)
#             for color in colors_str:
#                 query = query.filter(CardDetails.colors.ilike(f'%{color}%'))
#         elif color_match == 'at-most':
#             # Only the specified colors, but not necessarily all of them
#             for color in 'WUBRG':
#                 if color not in colors_str:
#                     query = query.filter(not_(CardDetails.colors.ilike(f'%{color}%')))
#
#     if cmc_min:
#         query = query.filter(CardDetails.cmc >= float(cmc_min))
#
#     if cmc_max:
#         query = query.filter(CardDetails.cmc <= float(cmc_max))
#
#     if rarity:
#         query = query.filter(CardDetails.rarity.in_(rarity))
#
#     if card_set:
#         sets = card_set.split(',')
#         query = query.filter(CardDetails.set_code.in_(sets))
#
#     if mtg_format:
#         # This depends on how you store format legality in your database
#         # For example, if you have a column named standard_legal
#         format_column = getattr(CardDetails, f"{mtg_format}_legal")
#         query = query.filter(format_column == True)
#
#     # Handle power/toughness for creatures
#     # Note: Since power/toughness can be non-numeric (like '*'), we need to be careful
#     if power_min:
#         # Filter only numeric power values greater than or equal to power_min
#         query = query.filter(CardDetails.power.op('REGEXP')('^[0-9]+$'))
#         query = query.filter(func.cast(CardDetails.power, Integer) >= int(power_min))
#
#     if power_max:
#         query = query.filter(CardDetails.power.op('REGEXP')('^[0-9]+$'))
#         query = query.filter(func.cast(CardDetails.power, Integer) <= int(power_max))
#
#     if toughness_min:
#         query = query.filter(CardDetails.toughness.op('REGEXP')('^[0-9]+$'))
#         query = query.filter(func.cast(CardDetails.toughness, Integer) >= int(toughness_min))
#
#     if toughness_max:
#         query = query.filter(CardDetails.toughness.op('REGEXP')('^[0-9]+$'))
#         query = query.filter(func.cast(CardDetails.toughness, Integer) <= int(toughness_max))
#
#     # Count total results for pagination before applying limits
#     total_cards = query.count()
#
#     # Add pagination
#     page = request.args.get('page', 1, type=int)
#     per_page = 30  # Cards per page
#     cards = query.order_by(CardDetails.name).offset((page - 1) * per_page).limit(per_page).all()
#
#     total_pages = (total_cards + per_page - 1) // per_page
#
#     # Close the session
#     session.close()
#
#     # Render the template with results and search parameters
#     return render_template(
#         'search.html',
#         cards=cards,
#         total_cards=total_cards,
#         page=page,
#         total_pages=total_pages,
#         search_params=request.args,
#         url_query_string=request.query_string.decode()
#     )
#


def handle_card_request(card_id_or_route):
    """
    Handler function to process card requests and create spot prices
    when appropriate, particularly for bot traffic
    """
    # 1. Detect if the request is from a bot
    is_bot = detect_bot_request(request)

    # 2. Process the card request normally
    card = get_card_data(card_id_or_route)

    # 3. If it's a bot or we need a spot price update, trigger price generation
    if is_bot or should_update_spot_price(card):
        # Run in background thread to not slow down the response
        threading.Thread(target=generate_spot_price, args=(card['id'],)).start()

    return render_card_page(card)

def detect_bot_request(request):
    """
    Detect if a request is likely from a bot based on user agent
    and other request characteristics
    """
    user_agent = request.headers.get('User-Agent', '').lower()

    # Common bot identifiers in user agents
    bot_identifiers = [
        'bot', 'crawler', 'spider', 'slurp', 'googlebot',
        'bingbot', 'yandex', 'baidu', 'semrush', 'ahrefsbot',
        'facebook', 'twitter', 'discordbot'
    ]

    # Check for bot identifiers in user agent
    if any(identifier in user_agent for identifier in bot_identifiers):
        return True

    # Check for missing/suspicious headers often associated with bots
    if not request.headers.get('Accept-Language'):
        return True

    # Check for unusual access patterns (optional, requires tracking)
    # if is_rapid_access_pattern(request.remote_addr):
    #     return True

    return False

def generate_spot_price(card_id):
    """
    Generate and store a spot price for a card
    """
    try:
        # Initialize MongoDB connection
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']

        # Get the card
        card = cards_collection.find_one({"id": card_id})
        if not card:
            logger.warning(f"Attempted to generate spot price for non-existent card: {card_id}")
            return

        # Check if we already have a recent spot price (within 24 hours)
        current_time = datetime.now()
        last_spot_price = card.get('spotPrice', {})

        if last_spot_price and 'timestamp' in last_spot_price:
            last_update = last_spot_price['timestamp']
            # If using string timestamps, convert to datetime
            if isinstance(last_update, str):
                try:
                    last_update = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                except:
                    last_update = current_time - datetime.timedelta(days=2)  # Force update

            # If we've updated within the last 24 hours, skip
            if (current_time - last_update).total_seconds() < 24 * 60 * 60:
                logger.info(f"Skipping spot price generation for {card.get('name')} - too recent")
                return

        # Calculate or fetch the spot price
        # This depends on your pricing strategy - you might:
        # 1. Query external APIs (TCGPlayer, CardKingdom, etc.)
        # 2. Use your own algorithm based on historical data
        # 3. Generate a price based on card attributes

        price_data = calculate_card_price(card)

        # Create the spot price object
        spot_price = {
            "timestamp": current_time,
            "price": price_data['price'],
            "currency": "USD",
            "source": "auto_generated_from_bot_visit"
        }

        # Update the card with the new spot price
        cards_collection.update_one(
            {"_id": card["_id"]},
            {"$set": {"spotPrice": spot_price}}
        )

        logger.info(f"Generated spot price of ${price_data['price']} for {card.get('name')}")

    except Exception as e:
        logger.error(f"Error generating spot price: {str(e)}")




@app.route('/card/rulings/<id_value>', methods=['GET'])
def get_card_rulings(id_value):
    """
    Get rulings for a card by either its Scryfall ID or TCGPlayer ID
    and store the rulings data array in the rulingsDetails field
    """
    # Initialize MongoDB connection
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client['mtgdbmongo']
    cards_collection = db['cards']

    # Try to determine what type of ID was provided
    card = None
    id_type = None

    # Check if it looks like a Scryfall ID (UUID format)
    if '-' in id_value and len(id_value) > 30:
        logger.info(f"Looking for card with Scryfall ID: {id_value}")
        card = cards_collection.find_one({"id": id_value})
        id_type = "Scryfall ID"

    # If not found, try to convert to integer for TCGPlayer ID
    if not card:
        try:
            tcgplayer_id = int(id_value)
            logger.info(f"Looking for card with TCGPlayer ID: {tcgplayer_id}")

            # Try different possible fields for TCGPlayer ID
            card = cards_collection.find_one({"tcgplayer_id": tcgplayer_id})

            if not card:
                card = cards_collection.find_one({"tcgplayer.id": tcgplayer_id})

            if not card:
                card = cards_collection.find_one({"identifiers.tcgplayer_id": str(tcgplayer_id)})

            if not card:
                card = cards_collection.find_one({"identifiers.tcgplayer": str(tcgplayer_id)})

            id_type = "TCGPlayer ID"
        except ValueError:
            # Not an integer, so not a TCGPlayer ID
            pass

    if not card:
        logger.error(f"Card not found with provided ID: {id_value}")
        return jsonify({
            "error": "Card not found",
            "details": "Could not find a card with the provided ID",
            "provided_id": id_value
        }), 404

    logger.info(f"Found card: {card.get('name', 'Unknown')} using {id_type}")

    # Check if we already have rulings data stored
    if card.get('rulingsDetails'):
        logger.info(f"Card already has rulings data stored in rulingsDetails")
        # Convert the MongoDB document to a Python dictionary with ObjectId converted to string
        card_dict = json.loads(json_util.dumps(card))
        return jsonify({
            "message": "Retrieved rulings from database",
            "card": card_dict
        })

    # Get the Scryfall ID for constructing the rulings URI
    scryfall_id = card.get('id')
    if not scryfall_id:
        logger.error(f"Card doesn't have a Scryfall ID")
        return jsonify({
            "error": "Missing Scryfall ID",
            "details": "The card was found but doesn't have a Scryfall ID needed for rulings"
        }), 500

    # Extract or construct the rulings URI
    rulings_uri = card.get('rulings_uri')

    if not rulings_uri:
        # Construct the rulings URI from the card ID
        rulings_uri = f"https://api.scryfall.com/cards/{scryfall_id}/rulings"
        logger.info(f"Constructed rulings URI: {rulings_uri}")

    # Make request to Scryfall API
    try:
        # Add a small delay to respect Scryfall's rate limits
        time.sleep(0.11)

        logger.info(f"Making request to: {rulings_uri}")
        response = requests.get(rulings_uri)
        response.raise_for_status()  # Raise an exception for HTTP errors

        rulings_data = response.json()

        # Extract only the data array from the rulings response
        rulings_details = rulings_data.get('data', [])

        # Process the rulings to prevent datetime-dictionary operations
        # Convert any datetime objects to strings to avoid operations between incompatible types
        processed_rulings = []
        for ruling in rulings_details:
            processed_ruling = {}
            # Safely copy each field
            for key, value in ruling.items():
                # Handle the published_at field specifically
                if key == 'published_at':
                    # Keep as string to avoid datetime operations
                    processed_ruling[key] = value
                # Handle any other datetime fields that might be in the data
                elif isinstance(value, dict):
                    # Make a deep copy of dictionaries to avoid reference issues
                    processed_ruling[key] = dict(value)
                else:
                    processed_ruling[key] = value
            processed_rulings.append(processed_ruling)

        ruling_count = len(processed_rulings)
        logger.info(f"Got response with {ruling_count} rulings")

        # Update the card document with the processed rulings
        cards_collection.update_one(
            {"_id": card["_id"]},
            {"$set": {"rulingsDetails": processed_rulings}}
        )

        # Fetch the updated card
        updated_card = cards_collection.find_one({"_id": card["_id"]})

        # Convert to dictionary with ObjectId converted to string
        updated_card_dict = json.loads(json_util.dumps(updated_card))

        # Return the updated card with rulings included
        return jsonify({
            "message": "Retrieved rulings from Scryfall API and stored in database",
            "card": updated_card_dict
        })

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch rulings: {str(e)}")
        return jsonify({
            "error": "Failed to fetch rulings",
            "details": str(e)
        }), 500
    except Exception as e:
        logger.error(f"Error processing rulings for card {card.get('name', 'Unknown')}: {str(e)}")
        # Log the full exception traceback for debugging
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            "error": "Failed to process rulings",
            "details": str(e)
        }), 500



@app.route('/update-all-rulings', methods=['POST'])
def update_all_rulings():
    """Update the rulings for all cards in the database"""
    # This could be a long-running task, consider implementing as a background job
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client['mtgdbmongo']
    cards_collection = db['cards']

    # Find all cards without rulingsData
    cards_without_rulings = list(cards_collection.find(
        {"rulingsData": {"$exists": False}},
        {"id": 1, "name": 1}
    ))

    logger.info(f"Found {len(cards_without_rulings)} cards without rulings data")

    updated_count = 0
    for card in cards_without_rulings:
        card_id = card.get('id')
        if not card_id:
            continue

        # Create the rulings URI
        rulings_uri = f"https://api.scryfall.com/cards/{card_id}/rulings"

        try:
            # Respect Scryfall's rate limits with a delay between requests
            time.sleep(0.1)

            response = requests.get(rulings_uri)
            if response.status_code == 200:
                rulings_data = response.json()

                # Update the card with the rulings data
                cards_collection.update_one(
                    {"id": card_id},
                    {"$set": {"rulingsData": rulings_data}}
                )

                updated_count += 1
                if updated_count % 10 == 0:
                    logger.info(f"Updated {updated_count} cards so far")
        except Exception as e:
            logger.error(f"Error updating rulings for card {card.get('name')}: {str(e)}")

    return jsonify({
        "message": f"Updated rulings for {updated_count} cards",
        "total_cards": len(cards_without_rulings)
    })


def fetch_single_card_spot_price(card_dict, db):
    try:
        card_name = card_dict.get('name', 'Unknown')
        logger.info(f"Starting spot price fetch for card: {card_name}")

        # Extract necessary collections
        products_collection = db['products']
        spotprices_collection = db['spotprices']

        # Extract Scryfall ID
        card_id = card_dict.get('id')
        if not card_id:
            logger.warning(f"No Scryfall ID found for card {card_name}")
            return None

        # Use date only (strip time) for timestamp
        # Get today's date
        today_date = datetime.now(timezone.utc)

        # Check if we already have a spotprice for this card today
        recent_spotprice = spotprices_collection.find_one({
            "card_id": card_id,
            "timestamp": today_date
        })

        if recent_spotprice:
            logger.info(f"Found spotprice from {recent_spotprice['timestamp'].strftime('%m/%d/%Y')} for {card_name}")
            existing_spotprice = True
        else:
            logger.info(f"No spotprice found for {card_name} today, will create new record")
            existing_spotprice = False

        # Extract TCGPlayer ID based on where it might be stored in the card data
        tcgplayer_id = None
        if "tcgplayer_id" in card_dict and card_dict["tcgplayer_id"]:
            tcgplayer_id = card_dict["tcgplayer_id"]
        elif "identifiers" in card_dict and "tcgplayer_id" in card_dict["identifiers"] and card_dict["identifiers"][
            "tcgplayer_id"]:
            tcgplayer_id = card_dict["identifiers"]["tcgplayer_id"]
        elif "tcgplayer" in card_dict and "id" in card_dict["tcgplayer"] and card_dict["tcgplayer"]["id"]:
            tcgplayer_id = card_dict["tcgplayer"]["id"]

        # Convert string IDs to integers if needed
        if isinstance(tcgplayer_id, str):
            try:
                tcgplayer_id_int = int(tcgplayer_id)
            except (ValueError, TypeError):
                tcgplayer_id_int = None
        else:
            tcgplayer_id_int = tcgplayer_id

        # Find the matching product using productId
        product = None
        game_id = None
        if tcgplayer_id_int:
            # Try integer version
            product = products_collection.find_one({"productId": tcgplayer_id_int})
            # Try string version if integer version fails
            if not product:
                product = products_collection.find_one({"productId": str(tcgplayer_id_int)})

        # If product not found by TCGPlayer ID, try other fields
        if not product:
            # Try Scryfall ID
            product = products_collection.find_one({"scryfall_id": card_id})
            # Try by name and set as fallback
            if not product and 'name' in card_dict and 'set' in card_dict:
                product = products_collection.find_one({
                    "name": card_dict['name'],
                    "set": card_dict['set']
                })

        # Extract game_id from product if available
        if product and 'game_id' in product:
            game_id = product['game_id']
            logger.info(f"Found game_id: {game_id} for card: {card_name}")

        # Extract prices directly from the card data if they exist
        scryfall_prices = {}
        if 'prices' in card_dict:
            scryfall_prices = card_dict.get('prices', {})
        else:
            # If prices not in card data, fetch from Scryfall API
            scryfall_url = f"https://api.scryfall.com/cards/{card_id}"
            response = requests.get(scryfall_url)
            if response.status_code == 200:
                scryfall_data = response.json()
                scryfall_prices = scryfall_data.get('prices', {})
            else:
                logger.warning(f"Failed to get data from Scryfall for card ID {card_id}: {response.status_code}")

        # Extract Scryfall prices
        usd = scryfall_prices.get('usd')
        usd_foil = scryfall_prices.get('usd_foil')
        usd_etched = scryfall_prices.get('usd_etched')
        eur = scryfall_prices.get('eur')
        eur_foil = scryfall_prices.get('eur_foil')
        tix = scryfall_prices.get('tix')

        # Clean up prices - convert to float or None
        clean_scryfall_prices = {
            "usd": float(usd) if usd else None,
            "usd_foil": float(usd_foil) if usd_foil else None,
            "usd_etched": float(usd_etched) if usd_etched else None,
            "eur": float(eur) if eur else None,
            "eur_foil": float(eur_foil) if eur_foil else None,
        }

        # Extract prices from product if we have one
        product_prices = {}
        if product:
            # Extract TCGPlayer price data
            if 'prices' in product:
                price_obj = product['prices']
                if isinstance(price_obj, dict):
                    for price_type, price_value in price_obj.items():
                        if price_value is not None:
                            try:
                                if isinstance(price_value, str):
                                    product_prices[price_type] = float(price_value)
                                else:
                                    product_prices[price_type] = price_value
                            except (ValueError, TypeError):
                                product_prices[price_type] = None

            # Check other common price fields
            price_fields = ['normal', 'foil', 'etched', 'marketPrice', 'buylistMarketPrice', 'buylistPrice',
                            'lowPrice', 'avgPrice', 'highPrice', 'latestPrice', 'midPrice']

            for field in price_fields:
                if field in product and product[field] is not None:
                    try:
                        if isinstance(product[field], str):
                            product_prices[field] = float(product[field])
                        else:
                            product_prices[field] = product[field]
                    except (ValueError, TypeError):
                        product_prices[field] = None

            # Check for other price fields
            for key in product.keys():
                if 'price' in key.lower() and key not in product_prices:
                    try:
                        if isinstance(product[key], str):
                            product_prices[key] = float(product[key])
                        else:
                            product_prices[key] = product[key]
                    except (ValueError, TypeError):
                        product_prices[key] = None

        # Check if we need to create/update the spotprice
        if existing_spotprice:
            # Compare prices to see if they've changed
            prices_changed = False

            # Compare Scryfall prices
            for price_key, new_price in clean_scryfall_prices.items():
                old_price = recent_spotprice.get('scryfall_prices', {}).get(price_key)
                if new_price != old_price:
                    logger.info(
                        f"Price change for {card_name}: Scryfall {price_key} changed from {old_price} to {new_price}")
                    prices_changed = True
                    break

            # Compare TCGPlayer prices
            if not prices_changed and product_prices:
                old_tcg_prices = recent_spotprice.get('tcgplayer_prices', {})
                # Check for new or changed prices
                for price_key, new_price in product_prices.items():
                    old_price = old_tcg_prices.get(price_key)
                    if new_price != old_price:
                        logger.info(
                            f"Price change for {card_name}: TCGPlayer {price_key} changed from {old_price} to {new_price}")
                        prices_changed = True
                        break

                # Check for removed prices
                if not prices_changed:
                    for price_key in old_tcg_prices:
                        if price_key not in product_prices:
                            logger.info(f"Price change for {card_name}: TCGPlayer {price_key} was removed")
                            prices_changed = True
                            break

            # Also update if game_id is different or missing
            if not prices_changed and game_id != recent_spotprice.get('game_id'):
                logger.info(f"Game ID change for {card_name}: from {recent_spotprice.get('game_id')} to {game_id}")
                prices_changed = True

            if not prices_changed:
                logger.info(f"No price changes detected for {card_name}, using existing spotprice")
                return recent_spotprice

            logger.info(f"Changes detected for {card_name}, will update existing spotprice")
            # Delete existing record to replace it with an updated one
            spotprices_collection.delete_one({"_id": recent_spotprice["_id"]})

        # Create spotprice document using today's date without time
        spotprice = {
            "card_id": card_id,
            "tcgplayer_id": tcgplayer_id_int if tcgplayer_id_int else tcgplayer_id,
            "game_id": game_id,
            "card_name": card_name,
            "set": card_dict.get('set', None),
            "collector_number": card_dict.get('collector_number', None),
            "rarity": card_dict.get('rarity', None),
            "scryfall_prices": clean_scryfall_prices,
            "tcgplayer_prices": product_prices,
            "timestamp": today_date
        }

        # Only save if we have any valid prices
        has_valid_scryfall_prices = any(p is not None for p in clean_scryfall_prices.values())
        has_valid_tcgplayer_prices = any(p is not None for p in product_prices.values())

        if not has_valid_scryfall_prices and not has_valid_tcgplayer_prices:
            logger.warning(f"No valid prices found for card {card_name}")
            return recent_spotprice if existing_spotprice else None

        # Insert into spotprices collection
        try:
            result = spotprices_collection.insert_one(spotprice)
            logger.info(
                f"Successfully inserted new spotprice with ID: {result.inserted_id} for date {today_date.strftime('%m/%d/%Y')}")
            return spotprice
        except Exception as db_error:
            logger.error(f"Database insertion error: {str(db_error)}")
            return recent_spotprice if existing_spotprice else None

    except Exception as e:
        logger.error(f"Error processing spot price for card {card_dict.get('name', 'Unknown')}: {str(e)}")
        logger.error(traceback.format_exc())
        return None


@app.route('/artists/<artist_name>')
def get_cards_by_artist(artist_name):
    # Initialize MongoDB connection
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client['mtgdbmongo']
    cards_collection = db['cards']

    cards = list(cards_collection.find(
        {"artist": artist_name}
    ).limit(67))

    # Render HTML template with the cards
    return render_template('artist.html', cards=cards, artist_name=artist_name)


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


@app.route('/card/<card_id>/<card_slug>')
@app.route('/card/<card_id>', defaults={'card_slug': None})
def card_detail(card_id, card_slug):
    start_time = time.time()
    # Detect bot
    is_bot = detect_bot_request(request)
    if is_bot:
        logger.info(f"Bot detected visiting card {card_id}, triggering spot price generation")
        threading.Thread(target=generate_spot_price, args=(card_id,)).start()


    try:
        # Initialize MongoDB connection
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']
        spotprices_collection = db['spotprices']

        # Try different approaches to find the card
        card = cards_collection.find_one({"id": card_id})

        # If that fails, try with ObjectId
        if card is None and len(card_id) == 24 and all(c in '0123456789abcdefABCDEF' for c in card_id):
            try:
                card = cards_collection.find_one({"_id": ObjectId(card_id)})
            except:
                pass

        # If still not found, try by slug
        if card is None and card_slug is not None:
            card = cards_collection.find_one({"slug": card_slug})

        # If still not found, try other fields
        if card is None:
            potential_id_fields = ["oracle_id", "mtgo_id", "arena_id", "tcgplayer_id"]
            for field in potential_id_fields:
                try:
                    card = cards_collection.find_one({field: card_id})
                    if not card:
                        card = cards_collection.find_one({field: int(card_id)})
                    if card:
                        break
                except (ValueError, TypeError):
                    continue

        if card is None:
            logger.error(f"Card not found for id: {card_id} and slug: {card_slug}")
            return "Card not found", 404

        # Process the card data to ensure all required fields exist
        card_dict = json.loads(json_util.dumps(card))

        # Ensure all nested structures exist
        if 'image_uris' not in card_dict:
            card_dict['image_uris'] = {
                'normal': '/static/img/card_back.png',
                'large': '/static/img/card_back.png',
                'art_crop': '/static/img/card_back.png'
            }

        # Add default values for other commonly accessed fields
        if 'normal_price' not in card_dict:
            card_dict['normal_price'] = None

        if 'mana_cost' not in card_dict:
            card_dict['mana_cost'] = ''

        # Fetch rulings from Scryfall API
        scryfall_id = card_dict.get('id')
        rulings_updated = False

        if scryfall_id:
            try:
                # Check when rulings were last updated
                last_updated = card_dict.get('rulings_last_updated')
                current_time = datetime.utcnow()

                # If rulings have never been updated or were updated more than 7 days ago, fetch them
                if not last_updated or (current_time - last_updated).days > 7:
                    rulings_url = f"https://api.scryfall.com/cards/{scryfall_id}/rulings"
                    response = requests.get(rulings_url)

                    if response.status_code == 200:
                        rulings_data = response.json()

                        # Update the card with new rulings
                        if 'data' in rulings_data and isinstance(rulings_data['data'], list):
                            # Update both the card_dict for current use and the database
                            card_dict['rulings'] = rulings_data['data']
                            card_dict['rulingsData'] = rulings_data
                            card_dict['rulingsDetails'] = rulings_data['data']
                            card_dict['rulings_last_updated'] = current_time

                            # Update the database with new rulings
                            cards_collection.update_one(
                                {"id": scryfall_id},
                                {"$set": {
                                    "rulings": rulings_data['data'],
                                    "rulingsData": rulings_data,
                                    "rulingsDetails": rulings_data['data'],
                                    "rulings_last_updated": current_time
                                }}
                            )

                            rulings_updated = True
                            logger.info(f"Rulings updated for card: {card_dict.get('name', 'Unknown')}")
                        else:
                            logger.warning(
                                f"No rulings data found in Scryfall response for card: {card_dict.get('name', 'Unknown')}")
                    else:
                        logger.warning(
                            f"Failed to fetch rulings from Scryfall for card: {card_dict.get('name', 'Unknown')}. Status code: {response.status_code}")
            except Exception as rulings_error:
                logger.error(
                    f"Error fetching rulings for card {card_dict.get('name', 'Unknown')}: {str(rulings_error)}")
                # Continue processing even if rulings fetch fails

        # Handle rulings - ensure compatibility with both old and new formats
        # If we didn't update rulings, use existing data
        if not rulings_updated:
            # Use rulingsDetails if available for rulings
            if 'rulingsDetails' in card_dict:
                card_dict['rulings'] = card_dict['rulingsDetails']
                # For backward compatibility, also set the data field in rulingsData
                if 'rulingsData' not in card_dict:
                    card_dict['rulingsData'] = {'data': card_dict['rulingsDetails']}
                else:
                    card_dict['rulingsData']['data'] = card_dict['rulingsDetails']
            elif 'rulings' not in card_dict:
                card_dict['rulings'] = []
                card_dict['rulingsData'] = {'data': []}

        # Get other printings and cards by the same artist if needed
        other_printings = []
        cards_by_artist = []

        # Only try to get these if we have the required fields
        if 'name' in card_dict and card_dict['name']:
            other_printings = list(cards_collection.find(
                {"name": card_dict['name'], "id": {"$ne": card_id}}
            ).limit(6))
            other_printings = json.loads(json_util.dumps(other_printings))

        if 'artist' in card_dict and card_dict['artist']:
            cards_by_artist = list(cards_collection.find(
                {"artist": card_dict['artist'], "id": {"$ne": card_id}}
            ).limit(6))
            cards_by_artist = json.loads(json_util.dumps(cards_by_artist))

        logger.info(f"Card found: {card_dict.get('name', 'Unknown')}")

        # Fetch and create spot price record for this card
        spot_price = fetch_single_card_spot_price(card_dict, db)

        # Get price history for this card
        price_history = None
        if 'id' in card_dict:
            # Get the last 30 days of price history
            price_history = list(spotprices_collection.find(
                {"card_id": card_dict['id']}
            ).sort("timestamp", -1).limit(30))
            price_history = json.loads(json_util.dumps(price_history))

        # If card_slug is None or doesn't match expected slug, redirect to the proper URL
        if card_slug is None and 'name' in card_dict and card_dict['name']:
            proper_slug = card_dict['name'].lower().replace(' ', '-').replace(',', '').replace("'", '')
            # Redirect to URL with proper slug
            from flask import redirect, url_for
            return redirect(url_for('card_detail', card_id=card_id, card_slug=proper_slug))

        # Pass all needed data to the template
        return render_template(
            'card_detail.html',
            card=card_dict,
            other_printings=other_printings,
            cards_by_artist=cards_by_artist,
            current_price=spot_price,
            price_history=price_history
        )

    except Exception as e:
        logger.error(f"Error in card_detail: {str(e)}")
        logger.error(traceback.format_exc())  # This will print the full stack trace
        return f"An error occurred: {str(e)}", 500

    finally:
        total_time = time.time() - start_time
        logger.info(f"Card detail request completed in {total_time:.2f} seconds")
        if 'client' in locals():
            client.close()



@app.route('/')
def index():
    client = None
    try:
        # Initialize MongoDB connection with a timeout
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']

        # ========================================================================================

        hero_card = fetch_random_card_from_db()

        # Safety check for hero_card
        if not hero_card:
            hero_card = {
                "id": "default",
                "name": "Magic Card",
                "image_uris": {
                    "normal": "/static/images/card-back.jpg"
                }
            }

        random_cards = list(cards_collection.find(
            {"tcgplayer_id": {"$ne": None}},
            {
                "_id": 1,
                "id": 1,
                "name": 1,
                "artist": 1,
                "oracle_text": 1,
                "printed_text": 1,
                "flavor_text": 1,
                "set_name": 1,
                "tcgplayer_id": 1,
                "normal_price": 1,
                "image_uris": 1  # Get the whole image_uris object
            }
        ).limit(67))

        return render_template('home.html', hero_card=hero_card, random_cards=random_cards)

    except Exception as e:
        import traceback
        print(f"Error in index route: {e}")
        print(traceback.format_exc())
        return f"An error occurred: {e}", 500
    finally:
        # Safely close the MongoDB client
        if client:
            client.close()


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


# @app.route('/asdf')
# def asdf():
#     client = MongoClient(os.getenv("MONGO_URI"))
#     db = client['mtgdbmongo']
#     cards_collection = db['cards']

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
        if 'client' in locals():
            client.close()
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
# SEO & Analytics Bots
User-agent: AhrefsBot
Disallow: /

User-agent: SemrushBot
Disallow: /

User-agent: MJ12bot
Disallow: /

User-agent: DotBot
Disallow: /

User-agent: DataForSeoBot
Disallow: /

User-agent: BLEXBot
Disallow: /


# Search Engine Bots (consider carefully)
User-agent: Baiduspider
Disallow: /

User-agent: YandexBot
Disallow: /

User-agent: YandexImages
Disallow: /

User-agent: PetalBot
Disallow: /

User-agent: SeznamBot
Disallow: /

User-agent: Mail.RU_Bot
Disallow: /


# Social Media & Scraper Bots
User-agent: FacebookBot
Disallow: /

User-agent: LinkedInBot
Disallow: /

User-agent: TelegramBot
Disallow: /

User-agent: TwitterBot
Disallow: /

User-agent: Pinterestbot
Disallow: /

User-agent: WhatsApp
Disallow: /


# Archive Bots
User-agent: ia_archiver
Disallow: /

User-agent: archive.org_bot
Disallow: /

# Aggressive Crawler Bots
User-agent: 360Spider
Disallow: /

User-agent: AspiegelBot
Disallow: /

User-agent: ZoominfoBot
Disallow: /

User-agent: CCBot
Disallow: /

User-agent: SentiBot
Disallow: /

User-agent: SerendeputyBot
Disallow: /

# AI & ML Training Bots
User-agent: GPTBot
Disallow: /

User-agent: ChatGPT-User
Disallow: /

User-agent: Google-Extended
Disallow: /

User-agent: anthropic-ai
Disallow: /

User-agent: Omgilibot
Disallow: /

User-agent: FacebookBot
Disallow: /

User-agent: Claude-Web
Disallow: /

# Block all bots from specific paths
User-agent: *
Disallow: /admin/
Disallow: /private/
Disallow: /api/
Disallow: /internal/

# Rate limiting hint (not officially supported but followed by some bots)
Crawl-delay: 10
""", mimetype='text/plain')



if __name__ == '__main__':
    app.run(debug=True, port=2357)

