import json
import logging
import os
import random
import ssl
import threading
from datetime import datetime, timedelta
import re
import math

import pymongo
from bson import ObjectId, json_util
from dotenv import load_dotenv
from flask import Flask, render_template
from flask import Response, send_from_directory
from flask import jsonify, request
from flask_caching import Cache
from flask_cors import CORS
from pymongo import MongoClient

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Also output to console
    ]
)
logger = logging.getLogger(__name__)


app = Flask(__name__)

# Configure Flask-Caching
# Make sure cache config comes before route definitions
cache_config = {
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 3600
}
cache = Cache()
cache.init_app(app, config=cache_config)  # Separate initialization



CONCURRENT_REQUESTS = 5  # Number of concurrent requests
REQUEST_TIMEOUT = 30  # Timeout in seconds
DELAY_BETWEEN_BATCHES = 1  # Delay between batches in seconds

# Define sitemap directory as a constant
SITEMAP_DIR = "static/sitemaps"


CORS(app)




# Get the MongoDB URI from the environment variable
mongo_uri = os.environ.get("MONGO_URI")

if mongo_uri:
    # client = MongoClient(mongo_uri)
    client = pymongo.MongoClient(
        mongo_uri,
        connectTimeoutMS=120000,  # Increase connection timeout
        socketTimeoutMS=45000,  # Increase socket timeout
        serverSelectionTimeoutMS=120000,  # Increase server selection timeout
        retryWrites=True,  # Enable retry for write operations
        tlsInsecure=False  # Ensure proper SSL validation
    )

else:
    # Fallback to a default URI or handle the error appropriately
    print("MONGO_URI environment variable not set!.")
    # Consider raising an exception or exiting if the URI is essential

db = client.get_database("mtgdbmongo")
cards_collection = db.get_collection("products")

db = client['mtgdbmongo']
# Define collection references
cards_collection = db['cards']
products_collection = db['products']
spotprices_collection = db['spotprices']


app.config['TEMPLATES_AUTO_RELOAD'] = True
app.secret_key = 'B87A0C9SQ54HBT3WBL-0998A3VNM09287NV0'


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



def generate_spot_price(card_id, force_update=False):
    """
    Generate spot price data for a card from Scryfall and product collection.

    Args:
        card_id: The Scryfall ID of the card
        force_update: If True, regenerates prices even if recently updated

    Returns:
        True if prices were successfully updated, False otherwise
    """
    try:
        # Check if we already have recent pricing and don't need to force update
        spot_price = spotprices_collection.find_one({"card_id": card_id}, sort=[("timestamp", -1)])
        current_time = datetime.now()

        # Save previous price data for comparison
        previous_prices = {
            "marketPrice": None,
            "usd": None,
            "eur": None
        }

        if spot_price and "prices" in spot_price:
            if "marketPrice" in spot_price["prices"]:
                previous_prices["marketPrice"] = spot_price["prices"]["marketPrice"]
            if "usd" in spot_price["prices"]:
                previous_prices["usd"] = spot_price["prices"]["usd"]
            if "eur" in spot_price["prices"]:
                previous_prices["eur"] = spot_price["prices"]["eur"]

        if not force_update and spot_price and "timestamp" in spot_price:
            last_updated = spot_price["timestamp"]
            # Only update prices once every 12 hours unless forced
            if (current_time - last_updated).total_seconds() < 43200:  # 12 hours in seconds
                print(f"Using cached prices for card {card_id} from {last_updated}")
                return False

        # Get card details from our database (which was populated from Scryfall)
        card = cards_collection.find_one({"id": card_id, "lang": "en"})
        if not card:
            print(f"Card {card_id} not found in database")
            return False

        # Get card name for top-level field
        card_name = card.get("name", "")

        # Initialize price data object
        price_data = {
            "card_id": card_id,
            "timestamp": current_time,
            "name": card_name,
            "prices": {},
            "metadata": {
                "set_code": card.get("set", ""),
                "collector_number": card.get("collector_number", ""),
                "rarity": card.get("rarity", ""),
                "name": card_name
            }
        }

        # Add tcgplayer_id if available
        if card.get("tcgplayer_id"):
            price_data["tcgplayer_id"] = card["tcgplayer_id"]

        # Step 1: Get Scryfall pricing data (USD and EUR)
        if "prices" in card:
            if card["prices"].get("usd"):
                price_data["prices"]["usd"] = float(card["prices"]["usd"])

            if card["prices"].get("eur"):
                price_data["prices"]["eur"] = float(card["prices"]["eur"])

        # Step 2: Try to match with TCGPlayer product data if tcgplayer_id exists
        if card.get("tcgplayer_id"):
            tcgplayer_updated = update_from_tcgplayer(price_data, card.get("tcgplayer_id"))
        # If no tcgplayer_id, try to match by name and set
        elif card.get("name") and card.get("set"):
            tcgplayer_id = find_tcgplayer_id_by_name_and_set(card.get("name"), card.get("set"))
            if tcgplayer_id:
                price_data["tcgplayer_id"] = tcgplayer_id
                tcgplayer_updated = update_from_tcgplayer(price_data, tcgplayer_id)

        # Only save the data if we have at least one price
        if price_data["prices"]:
            # Print the price comparison information
            print(f"Updated pricing for {card_name} ({card_id})")

            # Print marketPrice comparison if available
            market_price = price_data["prices"].get("marketPrice")
            print(f"marketPrice: From {previous_prices['marketPrice']} -> {market_price}")

            # Print USD comparison if available
            usd_price = price_data["prices"].get("usd")
            print(f"USD: From {previous_prices['usd']} -> {usd_price}")

            # Print EUR comparison if available
            eur_price = price_data["prices"].get("eur")
            print(f"EUR: From {previous_prices['eur']} -> {eur_price}")

            # Store the pricing data
            result = spotprices_collection.insert_one(price_data)
            return True
        else:
            print(f"No pricing data available for {card_name} ({card_id})")
            return False

    except Exception as e:
        import traceback
        print(f"Error generating spot price for {card_id}: {str(e)}")
        print(traceback.format_exc())
        return False



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



@app.template_filter('generate_slug')
def generate_slug(text):
    return text.lower().replace(' ', '-').replace(',', '').replace("'", '')



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



def update_from_tcgplayer(price_data, tcgplayer_id):
    """
    Update price_data with pricing from your products collection

    Args:
        price_data: The price data object to update
        tcgplayer_id: The TCGPlayer ID to match with

    Returns:
        True if prices were updated, False otherwise
    """
    try:
        # Query the products collection for the matching TCGPlayer product
        product = products_collection.find_one({"productId": tcgplayer_id})

        if not product:
            print(f"No matching product found for TCGPlayer ID {tcgplayer_id}")
            return False

        updated = False

        # Extract the pricing information from the product
        if "marketPrice" in product and product["marketPrice"] is not None:
            price_data["prices"]["market"] = product["marketPrice"]
            updated = True

        if "lowPrice" in product and product["lowPrice"] is not None:
            price_data["prices"]["low"] = product["lowPrice"]
            updated = True

        return updated

    except Exception as e:
        print(f"Error updating from TCGPlayer: {str(e)}")
        return False


def find_tcgplayer_id_by_name_and_set(card_name, set_code):
    """
    Find a TCGPlayer ID by matching card name and set

    Args:
        card_name: The name of the card
        set_code: The set code

    Returns:
        TCGPlayer ID if found, None otherwise
    """
    try:
        # First try to find the card in our cards collection
        card = cards_collection.find_one({
            "name": card_name,
            "set": set_code,
            "lang": "en",
            "tcgplayer_id": {"$exists": True, "$ne": None}
        })

        if card and card.get("tcgplayer_id"):
            return card.get("tcgplayer_id")

        # If not found, try to match directly in products collection
        product = products_collection.find_one({
            "name": {"$regex": f"^{re.escape(card_name)}$", "$options": "i"},
            "setCode": set_code.upper()  # Assuming setCode in products is uppercase
        })

        if product and product.get("productId"):
            return product.get("productId")

        return None

    except Exception as e:
        print(f"Error finding TCGPlayer ID: {str(e)}")
        return None



# @app.route('/card/rulings/<id_value>', methods=['GET'])
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


def fetch_single_card_spot_price(card_input, db=None):
    """
    Fetch the current market price for a single card.

    Parameters:
    card_input: Can be either a card dictionary or a card ID string
    db: Optional MongoDB database connection

    Returns:
    Dictionary with price information or None if not found
    """
    try:
        # Initialize variables
        card_name = "Unknown"
        card_id = None
        tcgplayer_id = None

        # Determine what type of input we received
        if isinstance(card_input, dict):
            # It's a card dictionary as expected
            card_name = card_input.get('name', 'Unknown')
            card_id = card_input.get('id')
            tcgplayer_id = card_input.get('tcgplayer_id')
        elif isinstance(card_input, str):
            # It's a card ID string
            card_id = card_input
            # Need to fetch card data from database
            if db is None:
                client = MongoClient(os.getenv("MONGO_URI"))
                db = client['mtgdbmongo']
            cards_collection = db['cards']
            card_doc = cards_collection.find_one({"id": card_id})
            if card_doc:
                card_name = card_doc.get('name', 'Unknown')
                tcgplayer_id = card_doc.get('tcgplayer_id')
        elif hasattr(card_input, 'find_one'):
            # It's a MongoDB collection object - likely a mistake
            logger.warning("Received Collection object instead of card data")
            # Attempt to fetch a random card as fallback
            card_doc = card_input.find_one()
            if card_doc:
                card_name = card_doc.get('name', 'Unknown')
                card_id = card_doc.get('id')
                tcgplayer_id = card_doc.get('tcgplayer_id')
        else:
            logger.error(f"Unexpected input type: {type(card_input)}")
            return None

        # Early return if no tcgplayer_id is available
        if not tcgplayer_id:
            logger.info(f"No tcgplayer_id found for card: {card_name}")
            return {
                "card_name": card_name,
                "card_id": card_id,
                "normal_price": None,
                "foil_price": None,
                "price_last_updated": None,
                "error": "No TCGPlayer ID available"
            }

        # Check cache first - using the global cache object from Flask-Caching
        cache_key = f"price_{tcgplayer_id}"
        cached_price = cache.get(cache_key)
        if cached_price:
            logger.debug(f"Cache hit for price of {card_name}")
            return cached_price

        # If not in cache, fetch from database or API
        if db is None:
            client = MongoClient(os.getenv("MONGO_URI"))
            db = client['mtgdbmongo']
        prices_collection = db['card_prices']

        # Look for recent price in database
        one_day_ago = datetime.now() - timedelta(days=1)
        recent_price = prices_collection.find_one({
            "tcgplayer_id": tcgplayer_id,
            "timestamp": {"$gt": one_day_ago}
        }, sort=[("timestamp", -1)])

        if recent_price:
            # Use recent price from database
            price_data = {
                "card_name": card_name,
                "card_id": card_id,
                "normal_price": recent_price.get('normal_price'),
                "foil_price": recent_price.get('foil_price'),
                "price_last_updated": recent_price.get('timestamp'),
                "source": "database"
            }
        else:
            # Fetch from TCGPlayer API
            price_data = fetch_price_from_tcgplayer_api(tcgplayer_id, card_name, card_id)

            # Store new price in database if API call succeeded
            if price_data and price_data.get('normal_price'):
                prices_collection.insert_one({
                    "tcgplayer_id": tcgplayer_id,
                    "normal_price": price_data.get('normal_price'),
                    "foil_price": price_data.get('foil_price'),
                    "timestamp": datetime.now(),
                    "card_id": card_id
                })

        # Cache the result for 6 hours
        if price_data:
            cache.set(cache_key, price_data, timeout=6 * 60 * 60)

        return price_data

    except Exception as e:
        # Safe error handling that won't cause another exception
        error_msg = f"Error processing spot price for card"
        if isinstance(card_input, dict) and 'name' in card_input:
            error_msg += f" {card_input['name']}"

        logger.error(f"{error_msg}: {str(e)}")
        return {
            "card_name": card_name if 'card_name' in locals() else "Unknown",
            "error": str(e)
        }

def fetch_price_from_tcgplayer_api(tcgplayer_id, card_name, card_id):
    """Helper function to fetch prices from TCGPlayer API"""
    try:
        # Your TCGPlayer API implementation here
        # This is a placeholder
        api_key = os.getenv("TCGPLAYER_API_KEY")
        if not api_key:
            return None

        # Example API call (replace with actual implementation)
        response = requests.get(
            f"https://api.tcgplayer.com/pricing/product/{tcgplayer_id}",
            headers={"Authorization": f"Bearer {api_key}"}
        )

        if response.status_code == 200:
            data = response.json()
            # Extract relevant price data
            return {
                "card_name": card_name,
                "card_id": card_id,
                "normal_price": extract_normal_price(data),
                "foil_price": extract_foil_price(data),
                "price_last_updated": datetime.now(),
                "source": "api"
            }
        else:
            logger.warning(f"TCGPlayer API returned status {response.status_code} for {card_name}")
            return None

    except Exception as e:
        logger.error(f"Error in TCGPlayer API call for {card_name}: {str(e)}")
        return None


@app.route('/artists/<artist_name>')
@cache.cached(timeout=300)
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
@cache.cached(timeout=300)
def art_gallery():
    hero_card = fetch_random_card_from_db()

    # Start background thread for price generation
    thread = threading.Thread(target=generate_spot_price, args=(hero_card['id'],))
    thread.daemon = False  # Ensure thread isn't a daemon
    thread.start()

    cards = list(cards_collection.aggregate([
        {'$match': {
            'image_uris.art_crop': {'$exists': True},
            'highres_image': True
        }},
        {'$sample': {'size': 373}},
        {'$project': {
            '_id': 0,
            'name': 1,
            'image_uris.art_crop': 1
        }}
    ]))

    print(f"Found {len(cards)} cards")

    if not cards:
        return "Error: cards not found.", 404

    print(cards[0])

    return render_template(
        'gallery.html',
        cards=cards
    )



@app.route('/card/<card_id>/<card_slug>')
@app.route('/card/<card_id>', defaults={'card_slug': None})
@cache.cached(timeout=3600)  # Cache for an hour
def card_detail(card_id, card_slug=None):
    """Card detail page with rulings"""
    try:
        # Get the card first
        card = cards_collection.find_one({"id": card_id})
        if not card:
            return render_template('error.html', message="Card not found"), 404

        # If card has tcgplayer_id, get the product pricing
        if card.get('tcgplayer_id'):
            # Look up product by tcgplayer_id
            product = products_collection.find_one({"productId": card['tcgplayer_id']})
            if product:
                # Add pricing data to card object
                card['price_data'] = {
                    'market_price': product.get('marketPrice'),
                    'low_price': product.get('lowPrice'),
                    'last_updated': product.get('lastUpdated', datetime.now())
                }
            else:
                card['price_data'] = None
                logger.warning(f"No product found for tcgplayer_id: {card['tcgplayer_id']}")

        # Start background thread for price generation
        thread = threading.Thread(target=generate_spot_price, args=(card_id,))
        thread.daemon = False
        thread.start()

        # Get rulings
        rulings = fetch_card_rulings(card_id)

        # Get other printings of the same card (English only)
        other_printings = []
        if card.get("oracle_id"):
            other_printings = list(cards_collection.find({
                "oracle_id": card.get("oracle_id"),
                "id": {"$ne": card_id},
                "lang": "en"  # English language filter
            }).sort("released_at", -1))

        # Get cards by the same artist (English only)
        cards_by_artist = []
        if card.get("artist"):
            cards_by_artist = list(cards_collection.find({
                "artist": card.get("artist"),
                "id": {"$ne": card_id},
                "lang": "en"  # English language filter
            }).limit(121))

        # Render template with all card data
        return render_template('card_detail.html',
                            card=card,
                            rulings=rulings,
                            other_printings=other_printings,
                            cards_by_artist=cards_by_artist)

    except Exception as e:
        logger.error(f"Error in card_detail: {str(e)}")
        return render_template('error.html', message="An error occurred"), 500




@app.route('/')
@cache.cached(timeout=600)
def index():
    try:
        # Use the already established MongoDB connection
        # This avoids creating a new connection for every request
        mongo_client = MongoClient(os.getenv("MONGO_URI"))
        db = mongo_client['mtgdbmongo']
        cards_collection = db['cards']

        hero_card = fetch_random_card_from_db()

        # Start background thread for price generation
        thread = threading.Thread(target=generate_spot_price, args=(hero_card['id'],))
        thread.daemon = False  # Ensure thread isn't a daemon
        thread.start()

        # Random card query with proper filters
        random_cmc = random.randint(0, 6)
        random_cards = list(cards_collection.aggregate([
            # Match stage (equivalent to your find filter)
            {'$match': {
                "tcgplayer_id": {"$ne": None},
                "lang": "en",
                "games": "paper",
                'highres_image': True
            }},
            # Sample stage
            {'$sample': {'size': 187}},
            # Project stage (equivalent to your projection)
            {'$project': {
                "_id": 1, "id": 1, "name": 1, "artist": 1,
                "oracle_text": 1, "printed_text": 1, "flavor_text": 1,
                "set_name": 1, "tcgplayer_id": 1, "normal_price": 1, "image_uris": 1
            }}
        ]))

        return render_template('home.html', hero_card=hero_card, random_cards=random_cards)

    except Exception as e:
        import traceback
        print(f"Error in index route: {e}")
        print(traceback.format_exc())
        return f"An error occurred: {e}", 500
    finally:
        # Safely close the MongoDB client connection
        if 'mongo_client' in locals():
            mongo_client.close()


@app.route('/_ah/health', methods=['HEAD', 'GET'])
def health_check():
    return Response('ok', status=200, mimetype='text/plain')

@app.route('/health', methods=['GET'])
def get_healthy():
    return Response('ok', status=200, mimetype='text/plain')



@app.route('/process_batch', methods=['GET'])
def process_batch():
    start_time = datetime.now()
    batch_id = start_time.strftime("%Y%m%d%H%M%S")

    start_continuous_updater()

    try:
        logger.info(f"[Batch-{batch_id}] Starting spot price batch processing")

        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']

        # Calculate the timestamp for 13 hours ago
        thirteen_hours_ago = datetime.now() - timedelta(hours=13)
        logger.info(f"[Batch-{batch_id}] Looking for cards not updated since {thirteen_hours_ago.isoformat()}")

        # Build the aggregation pipeline to find cards not recently updated
        pipeline = [
            {"$match": {
                "$or": [
                    {"spot_price_updated": {"$exists": False}},
                    {"spot_price_updated": {"$lt": thirteen_hours_ago}}
                ],
                # Make sure we have the Scryfall ID which is required for generate_spot_price
                "id": {"$exists": True},
                # Only process English cards as per the generate_spot_price logic
                "lang": "en"
            }},
            {"$sample": {"size": 513137}}
        ]

        # Execute the aggregation
        random_cards = list(cards_collection.aggregate(pipeline))

        logger.info(f"[Batch-{batch_id}] Selected {len(random_cards)} cards for spot price update")

        # Process each card
        processed_count = 0
        spot_prices_created = 0
        errors_count = 0
        skipped_count = 0

        for i, card in enumerate(random_cards, 1):
            card_id = card.get("id")  # This is the Scryfall ID
            card_name = card.get('name', 'unknown')
            card_set = card.get('set', 'unknown')
            card_number = card.get('collector_number', 'unknown')

            if not card_id:
                logger.warning(f"[Batch-{batch_id}] Card missing Scryfall ID: {card_name} - skipping")
                skipped_count += 1
                continue

            logger.info(
                f"[Batch-{batch_id}] [{i}/{len(random_cards)}] Processing {card_name} ({card_set}-{card_number})")

            try:
                # Call generate_spot_price with the Scryfall ID
                success = generate_spot_price(card_id)

                if success:
                    spot_prices_created += 1
                    logger.info(f"[Batch-{batch_id}] Successfully created spot price for {card_name}")
                else:
                    skipped_count += 1
                    logger.warning(f"[Batch-{batch_id}] No spot price data created for {card_name}")

                processed_count += 1

                # Mark this card as processed regardless of success
                cards_collection.update_one(
                    {"_id": card["_id"]},
                    {"$set": {"spot_price_updated": datetime.now()}}
                )

            except Exception as e:
                errors_count += 1
                logger.error(f"[Batch-{batch_id}] Error processing {card_name}: {str(e)}", exc_info=True)

        duration = datetime.now() - start_time

        # Log summary statistics
        logger.info(f"[Batch-{batch_id}] Batch processing completed in {duration.total_seconds():.2f} seconds")
        logger.info(
            f"[Batch-{batch_id}] Summary: {processed_count} processed, {spot_prices_created} spot prices created, {errors_count} errors, {skipped_count} skipped")

        return Response(
            f'Spot price batch update (Batch-{batch_id}) completed:\n'
            f'- {processed_count} cards processed\n'
            f'- {spot_prices_created} spot prices created/updated\n'
            f'- {errors_count} errors encountered\n'
            f'- {skipped_count} cards skipped\n'
            f'- {len(random_cards)} total cards selected\n'
            f'- Time elapsed: {duration.total_seconds():.2f} seconds',
            status=200,
            mimetype='text/plain'
        )

    except Exception as e:
        logger.error(f"[Batch-{batch_id}] Critical error in batch processing: {str(e)}", exc_info=True)
        return f"An error occurred: {e}", 500
    finally:
        if 'client' in locals() and client:
            client.close()
            logger.info(f"[Batch-{batch_id}] MongoDB connection closed")


def continuous_price_updater(mongo_uri):
    """
    Background thread that continuously updates spot prices in small batches
    with controlled delays between batches.
    """
    logger.info("Starting continuous price updater background thread")

    while True:
        client = None
        try:
            batch_size = 20  # Process 20 cards at a time
            min_delay = 60  # Minimum delay in seconds between batches

            # Get database connection
            client = MongoClient(mongo_uri)
            db = client['mtgdbmongo']
            cards_collection = db['cards']

            # Calculate the timestamp for cards to process
            twelve_hours_ago = datetime.now() - timedelta(hours=12)

            # Find cards to process
            pipeline = [
                {"$match": {
                    "$or": [
                        {"spot_price_updated": {"$exists": False}},
                        {"spot_price_updated": {"$lt": twelve_hours_ago}}
                    ],
                    "id": {"$exists": True},  # Must have Scryfall ID
                    "lang": "en"  # English cards only
                }},
                {"$sort": {"spot_price_updated": 1}},  # Oldest first (or null)
                {"$limit": batch_size}
            ]

            cards_to_process = list(cards_collection.aggregate(pipeline))

            if not cards_to_process:
                logger.info("No cards need spot price updates at this time. Sleeping for 5 minutes.")
                time.sleep(300)  # Sleep for 5 minutes if no cards need updates
                continue

            logger.info(f"Processing batch of {len(cards_to_process)} cards")

            processed_count = 0
            spot_prices_created = 0

            batch_start_time = time.time()

            # Process each card in the batch
            for card in cards_to_process:
                card_id = card.get("id")
                card_name = card.get('name', 'unknown')

                if not card_id:
                    logger.warning(f"Card missing Scryfall ID: {card_name} - skipping")
                    continue

                try:
                    # Generate spot price for this card
                    from main import generate_spot_price  # Import locally to avoid circular imports
                    success = generate_spot_price(card_id)

                    if success:
                        spot_prices_created += 1
                        logger.info(f"Created spot price for {card_name}")
                    else:
                        logger.warning(f"No spot price data created for {card_name}")

                    # Mark this card as processed regardless of success
                    cards_collection.update_one(
                        {"_id": card["_id"]},
                        {"$set": {"spot_price_updated": datetime.now()}}
                    )

                    processed_count += 1

                    # Small delay between individual cards to avoid hammering APIs
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Error processing {card_name}: {str(e)}", exc_info=True)

            # Calculate batch processing time
            batch_processing_time = time.time() - batch_start_time
            logger.info(f"Batch completed: {processed_count} processed, {spot_prices_created} spot prices created")

            # Calculate how long to wait before the next batch
            wait_time = max(min_delay - batch_processing_time, 0)
            if wait_time > 0:
                logger.info(f"Waiting {wait_time:.1f} seconds before next batch")
                time.sleep(wait_time)

        except Exception as e:
            logger.error(f"Error in continuous price updater: {str(e)}", exc_info=True)
            # If there's an error, wait a bit longer before retrying
            time.sleep(300)  # 5 minutes
        finally:
            # Close MongoDB connection
            if client:
                client.close()


# Function to start the background thread when your application starts
def start_continuous_updater():
    thread = threading.Thread(target=continuous_price_updater, daemon=True)
    thread.start()
    logger.info("Continuous price updater thread started")




def query_gemma(prompt, model="gemma3:4b"):
    """Query the local Ollama Gemma model"""
    response = requests.post('http://localhost:11434/api/generate',
                             json={
                                 'model': model,
                                 'prompt': prompt,
                                 'stream': False,
                                 'format': 'json'  # Request JSON output
                             })

    if response.status_code == 200:
        result = response.json()
        return result['response']
    else:
        return f"Error: {response.status_code}, {response.text}"

def create_mtg_search_prompt(user_query):
    return f"""
    You are a Magic: The Gathering card search assistant. 
    Convert the following natural language query into a MongoDB query object.
    
    For reference, the card schema includes:
    - name (string): Card name
    - colors (array): Card colors (e.g., ["W", "U", "B", "R", "G"])
    - type (string): Card type line (e.g., "Creature - Human Wizard")
    - text (string): Rules text
    - cmc (number): Converted mana cost / mana value
    - power (string/number): Creature power
    - toughness (string/number): Creature toughness
    - rarity (string): Card rarity
    - set (string): Set code
    - artist (string): Artist name
    
    USER QUERY: "{user_query}"
    
    Return ONLY a valid JSON object with MongoDB query parameters. For regex searches, use {{"$regex": "term", "$options": "i"}} format.
    """

@app.route('/gemma-search', methods=['GET', 'POST'])
def gemma_search():
    if request.method == 'POST':
        user_query = request.form.get('query')

        # Create prompt for Gemma
        prompt = create_mtg_search_prompt(user_query)

        # Query Gemma through Ollama
        try:
            llm_response = query_gemma(prompt)
            # Parse JSON response - with error handling
            try:
                mongo_query = json.loads(llm_response)
            except json.JSONDecodeError:
                # If Gemma doesn't return proper JSON, extract JSON substring
                import re
                json_pattern = r'\{.*\}'
                match = re.search(json_pattern, llm_response, re.DOTALL)
                if match:
                    mongo_query = json.loads(match.group(0))
                else:
                    return render_template('search_error.html', error="Could not parse Gemma response")

            # Execute MongoDB query
            client = MongoClient(os.getenv("MONGO_URI"))
            db = client['mtgdbmongo']
            cards_collection = db['cards']

            results = list(cards_collection.find(mongo_query).limit(20))

            return render_template('search_results.html',
                                   cards=results,
                                   query=user_query,
                                   mongo_query=json.dumps(mongo_query, indent=2))

        except Exception as e:
            return render_template('search_error.html', error=str(e))

    return render_template('search_form.html')


# Troubleshooting the rulings fetch
import requests
import time

def fetch_card_rulings(card_id, force_update=False):
    """
    Fetch rulings for a card from Scryfall API and save to the card document.

    Args:
        card_id: The Scryfall ID of the card
        force_update: If True, fetches from API even if we have cached rulings

    Returns:
        List of rulings for the card
    """
    # Check if we already have recent rulings in the database
    card = cards_collection.find_one({"id": card_id})

    if not card:
        print(f"Card {card_id} not found in database")
        return []

    # Check if we already have recent rulings and don't need to force update
    if not force_update and card.get("rulings") and card.get("rulings_last_updated"):
        last_updated = card.get("rulings_last_updated")
        if last_updated and (datetime.now() - last_updated).days < 30:
            print(f"Using cached rulings for card {card_id}")
            return card.get("rulings", [])

    # Fetch fresh rulings from Scryfall
    try:
        # Use proper URL format for rulings endpoint
        url = f"https://api.scryfall.com/cards/{card_id}/rulings"

        # Add logging for debugging
        print(f"Fetching rulings from: {url}")

        # Add proper headers and respect rate limits
        headers = {
            'User-Agent': 'MTGAppName/1.0 (your@email.com)'  # Replace with your app info
        }

        # Make the request with proper error handling
        response = requests.get(url, headers=headers)

        # Check if we hit a rate limit
        if response.status_code == 429:
            # Wait and retry once
            print("Rate limited, waiting 1 second...")
            time.sleep(1)
            response = requests.get(url, headers=headers)

        # Validate response
        response.raise_for_status()

        # Parse the response
        data = response.json()

        # Check if we got rulings data
        if 'data' in data:
            rulings = data['data']
            rulings_count = len(rulings)
            print(f"Found {rulings_count} rulings for card {card_id}")

            # Update the card document with rulings data
            cards_collection.update_one(
                {"id": card_id},
                {
                    "$set": {
                        "rulings": rulings,  # Store in card.rulings
                        "rulings_count": rulings_count,
                        "rulings_last_updated": datetime.now()
                    }
                }
            )

            return rulings
        else:
            print("No 'data' field in rulings response")
            # Save empty rulings to avoid repeated failed requests
            cards_collection.update_one(
                {"id": card_id},
                {
                    "$set": {
                        "rulings": [],  # Store empty list in card.rulings
                        "rulings_count": 0,
                        "rulings_last_updated": datetime.now()
                    }
                }
            )
            return []

    except requests.exceptions.RequestException as e:
        print(f"Error fetching rulings: {str(e)}")
        # Return existing rulings if available
        if card and card.get("rulings"):
            return card.get("rulings", [])
        return []
    except ValueError as e:
        print(f"Error parsing rulings JSON: {str(e)}")
        return []



# Batch processing function to populate rulings for all cards
def populate_rulings_for_all_cards(batch_size=100, delay_seconds=0.1):
    """
    Populate rulings for all cards in the database.
    Use this for initial population or periodic full updates.

    Args:
        batch_size: Number of cards to process in each batch
        delay_seconds: Delay between API calls to avoid rate limiting
    """
    from datetime import timedelta

    # Get all cards that don't have rulings or haven't been updated recently
    thirty_days_ago = datetime.now() - timedelta(days=30)

    # Find cards needing rulings update
    cards_needing_rulings = list(cards_collection.find(
        {
            "$or": [
                {"rulings_data": {"$exists": False}},
                {"rulings_last_updated": {"$lt": thirty_days_ago}}
            ]
        },
        {"id": 1}
    ).limit(1000))  # Process in chunks of 1000 cards

    print(f"Found {len(cards_needing_rulings)} cards needing rulings updates")

    # Process in batches with rate limiting
    for i in range(0, len(cards_needing_rulings), batch_size):
        batch = cards_needing_rulings[i:i + batch_size]
        print(
            f"Processing batch {i // batch_size + 1} of {(len(cards_needing_rulings) + batch_size - 1) // batch_size}")

        for card in batch:
            card_id = card.get("id")
            if card_id:
                # Fetch and save rulings
                fetch_card_rulings(card_id)

                # Wait to avoid rate limiting
                time.sleep(delay_seconds)

        print(f"Completed batch {i // batch_size + 1}")


async def fetch_url(session, url):
    """Fetch a single URL and return the status"""
    try:
        start_time = time.time()
        async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
            elapsed = time.time() - start_time
            status = response.status
            logger.info(f"Warmed up {url} - Status: {status} - Time: {elapsed:.2f}s")
            return {
                "url": url,
                "status": status,
                "time": elapsed
            }
    except Exception as e:
        logger.error(f"Error warming up {url}: {str(e)}")
        return {
            "url": url,
            "status": "Error",
            "time": 0,
            "error": str(e)
        }


def generate_card_sitemaps(base_url: str, sitemap_dir: str = "sitemaps") -> str:    
    """Generate sitemap files for cards and a sitemap index"""
    try:
        # Connect to MongoDB
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']
        
        # Log some info
        logger.info("Connected to MongoDB successfully")
        total_cards = cards_collection.count_documents({"lang": "en"})
        logger.info(f"Found {total_cards} English cards to process")

        # Create sitemaps directory if it doesn't exist
        sitemap_dir = os.path.abspath(sitemap_dir)
        os.makedirs(sitemap_dir, exist_ok=True)
        logger.info(f"Using sitemaps directory: {sitemap_dir}")

        # Constants
        URLS_PER_SITEMAP = 9999
        current_date = datetime.now().strftime("%Y-%m-%d")

        # Calculate total sitemaps needed
        total_sitemaps = math.ceil(total_cards / URLS_PER_SITEMAP)
        logger.info(f"Will generate {total_sitemaps} sitemap files")

        sitemap_files = []

        # Generate individual sitemaps
        for i in range(total_sitemaps):
            sitemap_name = f"sitemap-cards-{i+1}.xml"
            sitemap_path = os.path.join(sitemap_dir, sitemap_name)
            logger.info(f"Generating sitemap {i+1}/{total_sitemaps}: {sitemap_name}")

            # Get batch of cards
            cards = cards_collection.find(
                {"lang": "en"},
                {"id": 1, "name": 1, "updated_at": 1}
            ).skip(i * URLS_PER_SITEMAP).limit(URLS_PER_SITEMAP)

            # Generate sitemap content
            sitemap_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
            sitemap_content += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'

            url_count = 0
            for card in cards:
                # Generate URL-safe slug from card name
                slug = card.get("name", "").lower().replace(" ", "-").replace(",", "").replace("'", "")
                
                # Get last modified date
                lastmod = card.get("updated_at", datetime.now()).strftime("%Y-%m-%d")

                sitemap_content += f"""  <url>
    <loc>{base_url}/card/{card['id']}/{slug}</loc>
    <lastmod>{lastmod}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>\n"""
                url_count += 1

            sitemap_content += '</urlset>'

            # Write sitemap file
            with open(sitemap_path, 'w', encoding='utf-8') as f:
                f.write(sitemap_content)

            logger.info(f"Wrote {url_count} URLs to {sitemap_name}")

            sitemap_files.append({
                'filename': sitemap_name,
                'path': sitemap_path,
                'url': f"{base_url}/sitemaps/{sitemap_name}"
            })

        # Generate sitemap index
        logger.info("Generating sitemap index...")
        index_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
        index_content += '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'

        for sitemap in sitemap_files:
            index_content += f"""  <sitemap>
    <loc>{sitemap['url']}</loc>
    <lastmod>{current_date}</lastmod>
  </sitemap>\n"""

        index_content += '</sitemapindex>'

        # Write sitemap index
        index_path = os.path.join(sitemap_dir, 'sitemap.xml')
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(index_content)

        logger.info(f"Sitemap index written to {index_path}")
        logger.info(f"Generated {len(sitemap_files)} sitemap files with {total_cards} total URLs")

        return index_path

    except Exception as e:
        logger.error(f"Error generating sitemaps: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise

    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")


def generate_card_sitemaps(base_url: str, sitemap_dir: str = "sitemaps") -> str:    
    """Generate sitemap files for cards and a sitemap index"""
    try:
        # Connect to MongoDB
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']
        
        # Log some info
        logger.info("Connected to MongoDB successfully")
        total_cards = cards_collection.count_documents({"lang": "en"})
        logger.info(f"Found {total_cards} English cards to process")

        # Create sitemaps directory if it doesn't exist
        sitemap_dir = os.path.abspath(sitemap_dir)
        os.makedirs(sitemap_dir, exist_ok=True)
        logger.info(f"Using sitemaps directory: {sitemap_dir}")

        # Constants
        URLS_PER_SITEMAP = 50000
        current_date = datetime.now().strftime("%Y-%m-%d")

        # Calculate total sitemaps needed
        total_sitemaps = math.ceil(total_cards / URLS_PER_SITEMAP)
        logger.info(f"Will generate {total_sitemaps} sitemap files")

        sitemap_files = []

        # Generate individual sitemaps
        for i in range(total_sitemaps):
            sitemap_name = f"sitemap-cards-{i+1}.xml"
            sitemap_path = os.path.join(sitemap_dir, sitemap_name)
            logger.info(f"Generating sitemap {i+1}/{total_sitemaps}: {sitemap_name}")

            # Get batch of cards
            cards = cards_collection.find(
                {"lang": "en"},
                {"id": 1, "name": 1, "updated_at": 1}
            ).skip(i * URLS_PER_SITEMAP).limit(URLS_PER_SITEMAP)

            # Generate sitemap content
            sitemap_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
            sitemap_content += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'

            url_count = 0
            for card in cards:
                # Generate URL-safe slug from card name
                slug = card.get("name", "").lower().replace(" ", "-").replace(",", "").replace("'", "")
                
                # Get last modified date
                lastmod = card.get("updated_at", datetime.now()).strftime("%Y-%m-%d")

                sitemap_content += f"""  <url>
    <loc>{base_url}/card/{card['id']}/{slug}</loc>
    <lastmod>{lastmod}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>\n"""
                url_count += 1

            sitemap_content += '</urlset>'

            # Write sitemap file
            with open(sitemap_path, 'w', encoding='utf-8') as f:
                f.write(sitemap_content)

            logger.info(f"Wrote {url_count} URLs to {sitemap_name}")

            sitemap_files.append({
                'filename': sitemap_name,
                'path': sitemap_path,
                'url': f"{base_url}/sitemaps/{sitemap_name}"
            })

        # Generate sitemap index
        logger.info("Generating sitemap index...")
        index_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
        index_content += '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'

        for sitemap in sitemap_files:
            index_content += f"""  <sitemap>
    <loc>{sitemap['url']}</loc>
    <lastmod>{current_date}</lastmod>
  </sitemap>\n"""

        index_content += '</sitemapindex>'

        # Write sitemap index
        index_path = os.path.join(sitemap_dir, 'sitemap.xml')
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(index_content)

        logger.info(f"Sitemap index written to {index_path}")
        logger.info(f"Generated {len(sitemap_files)} sitemap files with {total_cards} total URLs")

        return index_path

    except Exception as e:
        logger.error(f"Error generating sitemaps: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise

    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

@app.route('/sitemap.xml')
def sitemap_index():
    """Serve the sitemap index file"""
    try:
        # Generate sitemaps if they don't exist
        if not os.path.exists(os.path.join(SITEMAP_DIR, 'sitemap.xml')):
            logger.info("Sitemap index not found, generating sitemaps...")
            generate_card_sitemaps(
                base_url="https://www.tcgplex.com", 
                sitemap_dir=SITEMAP_DIR
            )
        
        logger.info(f"Serving sitemap index from {SITEMAP_DIR}")
        return send_from_directory(SITEMAP_DIR, 'sitemap.xml')
    except Exception as e:
        logger.error(f"Error serving sitemap index: {str(e)}")
        return str(e), 500

@app.route('/sitemaps/<path:filename>')
def serve_sitemap(filename):
    """Serve individual sitemap files"""
    try:
        logger.info(f"Attempting to serve sitemap file: {filename} from {SITEMAP_DIR}")
        return send_from_directory(SITEMAP_DIR, filename)
    except Exception as e:
        logger.error(f"Error serving sitemap file {filename}: {str(e)}")
        return str(e), 404

@app.route('/generate-sitemaps')
def generate_sitemaps():
    """Force regeneration of all sitemaps"""
    try:
        logger.info("Starting sitemap generation...")
        # Make sure the static/sitemaps directory exists
        os.makedirs(SITEMAP_DIR, exist_ok=True)
        
        result = generate_card_sitemaps(
            base_url="https://www.tcgplex.com",
            sitemap_dir=SITEMAP_DIR
        )
        logger.info(f"Sitemaps generated successfully at: {result}")
        return f"Sitemaps generated at: {result}"
    except Exception as e:
        logger.error(f"Error generating sitemaps: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return f"Error generating sitemaps: {str(e)}", 500

@app.route('/asdfasdf')
def asdf():
    try:
        logger.info("Starting sitemap generation...")
        result = generate_card_sitemaps(base_url="https://tcgplex.com")
        logger.info(f"Sitemaps generated successfully at: {result}")
        return f"Sitemaps generated at: {result}"
    except Exception as e:
        logger.error(f"Error generating sitemaps: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return f"Error generating sitemaps: {str(e)}", 500