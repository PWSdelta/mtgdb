import json
import logging
import os
import random
import threading
import time
import traceback
from datetime import datetime, timedelta

import requests
from bson import ObjectId, json_util
from dotenv import load_dotenv
from flask import Flask, render_template
from flask import Response
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




CORS(app)


# Get the MongoDB URI from the environment variable
mongo_uri = os.environ.get("MONGO_URI")

if mongo_uri:
    client = MongoClient(mongo_uri)
else:
    # Fallback to a default URI or handle the error appropriately
    print("MONGO_URI environment variable not set!.")
    # Consider raising an exception or exiting if the URI is essential

db = client.get_database("mtgdbmongo")  # Replace your_database_name if needed
cards_collection = db.get_collection("cards")

db = client['mtgdbmongo']
collection = db['cards']

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

def generate_spot_price(card_id):
    """
    Generate and store a spot price for a card
    """
    try:
        logger.info(f"Starting spot price generation for {card_id}")
        import sys
        sys.stdout.flush()  # Force flush output

        # Initialize MongoDB connection
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']
        spotprices_collection = db['spotprices']

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
                    last_update = current_time - timedelta(days=2)  # Force update

            # If we've updated within the last 24 hours, skip
            if (current_time - last_update).total_seconds() < 24 * 60 * 60:
                logger.info(f"Skipping spot price generation for {card.get('name')} - too recent")
                return

        # Get price data from the card
        base_price = 0.0

        # Try different price fields
        price_fields = ['normal_price', 'prices.usd', 'prices.usd_foil', 'price']
        for field in price_fields:
            # Handle nested fields like 'prices.usd'
            if '.' in field:
                parts = field.split('.')
                value = card
                for part in parts:
                    if isinstance(value, dict) and part in value:
                        value = value[part]
                    else:
                        value = None
                        break
                potential_price = value
            else:
                potential_price = card.get(field)

            # Try to convert to float
            if potential_price is not None:
                try:
                    if isinstance(potential_price, str):
                        potential_price = float(potential_price)
                    if isinstance(potential_price, (int, float)) and potential_price > 0:
                        base_price = potential_price
                        logger.info(f"Using base price {base_price} from field {field}")
                        break
                except:
                    pass

        # Default price if none found
        if base_price <= 0:
            base_price = 1.0
            logger.info(f"No valid price found, using default: {base_price}")

        # Generate a spot price with some randomness
        import random
        variation = 0.9 + random.random() * 0.2  # ±10% variation
        spot_price_value = round(base_price * variation, 2)

        # Create the spot price record
        timestamp = datetime.now()
        spot_price = {
            "card_id": card_id,
            "card_name": card.get('name', 'Unknown'),
            "price": spot_price_value,
            "currency": "USD",
            "source": "Internal Algorithm",
            "timestamp": timestamp
        }

        # Insert into spotprices collection
        spotprices_collection.insert_one(spot_price)

        # Update the card with the spot price
        cards_collection.update_one(
            {"id": card_id},
            {"$set": {"spotPrice": spot_price}}
        )

        logger.info(f"Generated spot price of ${spot_price_value} for {card.get('name')}")
        logger.info(f"Completed spot price generation for {card_id}")
        sys.stdout.flush()  # Force flush again

    except Exception as e:
        logger.error(f"Error in spot price generation: {str(e)}")
        # Print stack trace for debugging
        import traceback
        logger.error(traceback.format_exc())
        sys.stdout.flush()  # Force flush on error too

    finally:
        # Ensure we close MongoDB connection
        if 'client' in locals():
            client.close()




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


def extract_normal_price(api_data):
    """Extract normal price from TCGPlayer API response"""
    # Implement based on actual API response structure
    # This is a placeholder
    return api_data.get('results', [{}])[0].get('marketPrice')


def extract_foil_price(api_data):
    """Extract foil price from TCGPlayer API response"""
    # Implement based on actual API response structure
    # This is a placeholder
    return api_data.get('results', [{}])[0].get('foilMarketPrice')


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
    cards = list(collection.aggregate([
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
@cache.cached(timeout=300)
def card_detail(card_id, card_slug):
    start_time = time.time()

    logger.info(f" {card_id}: Triggering spot price generation")

    thread = threading.Thread(target=generate_spot_price, args=(card_id,))
    thread.daemon = False  # Ensure thread isn't a daemon
    thread.start()

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

        # Get price history for this card
        price_history = None

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
            cards_by_artist=cards_by_artist
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
@cache.cached(timeout=300)
def index():
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']

        hero_card = fetch_random_card_from_db()

        thread = threading.Thread(target=generate_spot_price, args=(hero_card['id'],))
        thread.daemon = False  # Ensure thread isn't a daemon
        thread.start()

        random_cmc = random.randint(0, 6)
        random_cards = list(cards_collection.aggregate([
            # Match stage (equivalent to your find filter)
            {'$match': {
                "tcgplayer_id": {"$ne": None},
                "lang": "en",
                "games": "paper",
                "cmc": {"$lt": random_cmc}
            }},
            # Sample stage
            {'$sample': {'size': 167}},
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
        # Safely close the MongoDB client
        if client:
            client.close()

@app.route('/_ah/health', methods=['HEAD', 'GET'])
def health_check():
    return Response('ok', status=200, mimetype='text/plain')

@app.route('/health', methods=['GET'])
def get_healthy():
    return Response('ok', status=200, mimetype='text/plain')



@app.route('/asdf', methods=['GET', 'HEAD'])
def asdf():
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client['mtgdbmongo']
        cards_collection = db['cards']
        card = fetch_random_card_from_db()

        fetch_single_card_spot_price(db)

        logger.info(f"Inserted spot price for {card['name']}")

        return Response('ok', status=200, mimetype='text/plain')

    except Exception as e:
        import traceback
        print(f"Error in index route: {e}")
        print(traceback.format_exc())
        return f"An error occurred: {e}", 500
    finally:
        if client:
            client.close()



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