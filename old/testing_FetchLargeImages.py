import psycopg2
import requests
import os
import re




DB_CONFIG = {
    'host': 'localhost',
    'database': 'mtgdb',
    'user': 'postgres',
    'password': 'asdfghjkl',  # Replace with your actual password
    'port': '5432'
}

# Folder to save images
SAVE_DIR = 'images/'


# Function to sanitize filenames (remove invalid characters)
def sanitize_filename(filename):
    """
    Sanitizes a filename by removing or replacing invalid characters.
    """
    # Replace invalid characters with an underscore
    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
    # Remove leading and trailing whitespace
    filename = filename.strip()
    return filename


# Function to create a unique index on the normal_image_name column
def create_uniqueness_constraint():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Check if the index already exists
        check_query = """
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'large_image_name_unique_idx';
        """
        cursor.execute(check_query)
        index_exists = cursor.fetchone() is not None

        if not index_exists:
            # Create a unique index on large_image_name
            create_index_query = """
            CREATE UNIQUE INDEX large_image_name_unique_idx 
            ON card_details (large_image_name) 
            WHERE large_image_name IS NOT NULL;
            """
            cursor.execute(create_index_query)
            conn.commit()
            print("Created unique index on large_image_name column.")
        else:
            print("Unique index on large_image_name already exists.")

        cursor.close()
        conn.close()
    except psycopg2.Error as db_error:
        print(f"Database error creating index: {db_error}")


# Function to download images and save them
def download_images():
    # Ensure the save directory exists
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

    # Connect to the database
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Updated query to include lang column
        query = """
            SELECT id, image_uris, name, set_name, tcgplayer_id, lang
            FROM card_details
            WHERE image_uris IS NOT NULL AND (large_image_name IS NULL OR normal_image_name = '');
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Iterate through each row
        for row in rows:
            card_id = row[0]
            image_uris = row[1]
            card_name = row[2]
            set_name = row[3]
            tcgplayer_id = row[4]
            lang = row[5]  # Language column

            # Skip any cards with None values for required fields
            if card_name is None or set_name is None:
                print(f"Skipping card ID {card_id} due to missing name or set name.")
                continue

            # Extract the `normal` key from the JSONB object
            if 'large' in image_uris:
                image_url = image_uris['large']

                try:
                    # Download the image
                    response = requests.get(image_url, stream=True)
                    response.raise_for_status()  # Raise an error for failed requests

                    # Sanitize the card name and set name
                    card_name_cleaned = sanitize_filename(card_name.replace(" ", "_").lower())
                    set_name_cleaned = sanitize_filename(set_name.replace(" ", "_").lower())

                    # Handle potentially None values with fallbacks
                    lang_cleaned = sanitize_filename(lang.lower()) if lang is not None else "unknown"
                    tcgplayer_str = str(tcgplayer_id) if tcgplayer_id is not None else "notcg"

                    # Construct the filename in the new order: name, lang, tcgplayer_id, set, "image.jpg"
                    filename = f"{card_name_cleaned}_{lang_cleaned}_{tcgplayer_str}_{set_name_cleaned}_mtg.jpg"

                    # Full file path to save the image
                    filepath = os.path.join(SAVE_DIR, filename)

                    # Save the image to the specified directory
                    with open(filepath, 'wb') as image_file:
                        for chunk in response.iter_content(1024):
                            image_file.write(chunk)

                    print(f"Successfully downloaded: {filename}")

                    # Python code snippet
                    try:
                        update_query = """
                            UPDATE card_details
                            SET normal_image_name = %s
                            WHERE id = %s;
                        """
                        cursor.execute(update_query, (filename, card_id))
                        conn.commit()
                    except psycopg2.Error as db_error:
                        # Check if it's a duplicate key error by inspecting the error message
                        if "duplicate key value violates unique constraint" in str(db_error):
                            print(
                                f"Duplicate filename encountered for card ID {card_id} with filename {filename}; skipping update.")
                            conn.rollback()
                        else:
                            # For all other errors, re-raise or handle accordingly
                            raise


                except requests.exceptions.RequestException as e:
                    print(f"Failed to download image for ID {card_id}: {e}")
            else:
                print(f"No 'normal' image URL found for ID {card_id}.")

        # Close the database connection
        cursor.close()
        conn.close()

    except psycopg2.Error as db_error:
        print(f"Database error: {db_error}")


# Run the script
if __name__ == '__main__':
    create_uniqueness_constraint()  # Create the uniqueness constraint first
    download_images()  # Then download the images
