import psycopg2
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_CONNECTION_STRING = os.environ.get('LOCAL_DB_URL')


def process_cards(json_file_path):
    try:
        # Read the JSON file
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Connect to the PostgreSQL database
        with psycopg2.connect(DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:
                # Fetch existing IDs from the database
                cursor.execute("SELECT id FROM card_details;")
                existing_ids = {row[0] for row in cursor.fetchall()}  # Use a set for fast lookups
                print(f"Fetched {len(existing_ids)} existing IDs from the database.")

                # Filter out existing cards
                new_cards = [card for card in data if card.get('id') not in existing_ids]
                print(f"Filtered {len(new_cards)} new cards to insert.")

                # Prepare data for batch insertion
                insert_data = []
                for card in new_cards:
                    card_id = card.get('id')
                    card_name = card.get('card_name')
                    set_name = card.get('set_name')
                    image_urls = card.get('image_uris')

                    insert_data.append((card_id, card_name, set_name, json.dumps(image_urls)))

                # Batch insert the new records
                insert_query = "INSERT INTO card_details (id, card_name, set_name, image_uris) VALUES (%s, %s, %s, %s);"
                if insert_data:
                    psycopg2.extras.execute_batch(cursor, insert_query, insert_data)
                    conn.commit()
                    print(f"Inserted {len(insert_data)} new records into the database.")
                else:
                    print("No new records to insert.")

    except Exception as e:
        print(f"An error occurred: {e}")


# Example usage
json_file_path = 'all-cards.json'  # Replace with the actual path
process_cards(json_file_path)