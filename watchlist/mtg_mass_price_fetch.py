import os

import requests
import time
from pymongo import MongoClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ScryfallPriceUpdater:
    def __init__(self, mongo_uri=os.getenv('MONGO_URI'), db_name="mtgdbmongo", mtg_game_id=1, set_code=None):
        """Initialize the ScryfallPriceUpdater with MongoDB connection parameters."""
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.mtg_game_id = mtg_game_id  # Game ID for Magic: The Gathering
        self.set_code = set_code  # Optional set code to filter by
        self.client = None
        self.db = None
        self.products = None

    def connect(self):
        """Connect to MongoDB database."""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            self.products = self.db["products"]
            logger.info("Connected to MongoDB")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise

    def disconnect(self):
        """Disconnect from MongoDB."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")

    def get_products_without_scryfall_price(self):
        """Get all products that have a TCGPlayer ID but no Scryfall USD price."""
        try:
            # Query for products with TCGPlayer IDs but no Scryfall USD price
            query = {
                "gameId": self.mtg_game_id,
                "productId": {"$exists": True, "$ne": None} #,
                # "$or": [
                #     {"scryfall_usd_price": {"$exists": False}},
                #     {"scryfall_usd_price": None}
                # ]
            }

            # Add set filter if specified
            if self.set_code:
                query["setCode"] = self.set_code

            products = list(self.products.find(query))

            if self.set_code:
                logger.info(f"Found {len(products)} products without Scryfall prices in set {self.set_code}")
            else:
                logger.info(f"Found {len(products)} products without Scryfall prices across all sets")

            return products
        except Exception as e:
            logger.error(f"Error fetching products: {e}")
            return []

    def get_scryfall_price_by_tcgplayer_id(self, tcgplayer_id):
        """
        Get price data from Scryfall for a specific TCGPlayer ID.
        Scryfall API allows looking up cards by TCGPlayer ID using the format:
        https://api.scryfall.com/cards/tcgplayer/{id}
        """
        url = f"https://api.scryfall.com/cards/tcgplayer/{tcgplayer_id}"
        logger.debug(f"Fetching Scryfall data for TCGPlayer ID: {tcgplayer_id}")

        try:
            response = requests.get(url)

            # Respect rate limits
            time.sleep(0.1)

            if response.status_code == 200:
                card_data = response.json()
                prices = card_data.get('prices', {})

                # Extract only the pricing we want
                price_data = {
                    "scryfall_id": card_data.get('id'),
                    "scryfall_uri": card_data.get('uri'),
                    "scryfall_usd_price": float(prices.get('usd')) if prices.get('usd') else None,
                    "scryfall_eur_price": float(prices.get('eur')) if prices.get('eur') else None
                }

                logger.debug(
                    f"Price data for {card_data.get('name')}: USD=${price_data['scryfall_usd_price']}, EUR€{price_data['scryfall_eur_price']}")
                return price_data
            elif response.status_code == 404:
                logger.warning(f"TCGPlayer ID {tcgplayer_id} not found in Scryfall")
                return None
            else:
                logger.error(
                    f"Error fetching data for TCGPlayer ID {tcgplayer_id}: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.error(f"Exception fetching TCGPlayer ID {tcgplayer_id}: {e}")
            return None

    def format_price(self, price, currency="USD"):
        """Format price as currency string or return 'N/A' if None."""
        if price is None:
            return "N/A"
        if currency.upper() == "USD":
            return f"${price:.2f}"
        elif currency.upper() == "EUR":
            return f"€{price:.2f}"
        return f"{price:.2f} {currency}"

    def update_missing_prices(self):
        """Update products that don't have Scryfall prices yet."""
        products = self.get_products_without_scryfall_price()

        if not products:
            set_info = f" for set {self.set_code}" if self.set_code else ""
            logger.warning(f"No products without Scryfall prices found in database{set_info}")
            return 0

        updated_count = 0
        not_found_count = 0
        error_count = 0

        for product in products:
            product_id = product.get('productId')
            product_name = product.get('name', 'Unknown')

            if not product_id:
                logger.debug(f"Skipping product with no TCGPlayer ID: {product_name}")
                continue

            price_data = self.get_scryfall_price_by_tcgplayer_id(product_id)

            if not price_data:
                logger.warning(f"No price data found for: {product_name} (TCGPlayer ID: {product_id})")
                not_found_count += 1
                continue

            try:
                # Only update fields that have values
                update_fields = {k: v for k, v in price_data.items() if v is not None}

                if update_fields:
                    self.products.update_one(
                        {"_id": product["_id"]},
                        {"$set": update_fields}
                    )
                    updated_count += 1

                    # Format price updates for logging
                    usd_price_str = self.format_price(price_data.get('scryfall_usd_price'), 'USD')
                    eur_price_str = self.format_price(price_data.get('scryfall_eur_price'), 'EUR')

                    # Include TCGPlayer ID in the "Updated prices for" message
                    logger.info(
                        f"Added prices for: {product_name} (TCGPlayer ID: {product_id}) (USD: {usd_price_str}, EUR: {eur_price_str})")
                else:
                    logger.warning(f"No valid price fields found for: {product_name} (TCGPlayer ID: {product_id})")
                    not_found_count += 1
            except Exception as e:
                logger.error(f"Error updating product {product_name} (TCGPlayer ID: {product_id}): {e}")
                error_count += 1

        set_info = f" for set {self.set_code}" if self.set_code else ""
        logger.info(f"Added prices for {updated_count} products{set_info}")
        logger.info(f"Could not find data for {not_found_count} products{set_info}")
        logger.info(f"Encountered errors updating {error_count} products{set_info}")

        return updated_count

    def run(self):
        """Run the Scryfall price update process for products without prices."""
        try:
            self.connect()
            set_info = f" for set {self.set_code}" if self.set_code else ""
            logger.info(f"Starting price population process{set_info} for products without Scryfall prices")

            updated_count = self.update_missing_prices()

            logger.info(f"Successfully added Scryfall pricing data to {updated_count} products{set_info}")
        except Exception as e:
            logger.error(f"Error in price update process: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.disconnect()


if __name__ == "__main__":
    # Update all products without Scryfall prices
    # Can also target a specific set with set_code parameter
    updater = ScryfallPriceUpdater(
        mongo_uri="mongodb://localhost:27017/",
        db_name="tcgprime_db",
        mtg_game_id=1,
        set_code=None  # Set to None to process all sets, or specify a set code
    )
    updater.run()