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