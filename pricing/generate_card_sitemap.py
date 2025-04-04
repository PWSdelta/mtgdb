# Language: Python

import psycopg2
import os
from dotenv import load_dotenv
import xml.etree.ElementTree as ET

# Load environment variables (adjust variable name as needed)
load_dotenv()
DB_CONNECTION_STRING = os.environ.get('RW_DATABASE_URL')

# The base URL for your cards on your website.
BASE_URL = "https://pwsdelta.com"


def fetch_card_details():
    """Fetch card ids from the card_details table."""
    query = "SELECT id FROM card_details ORDER BY id;"
    try:
        with psycopg2.connect(DB_CONNECTION_STRING) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                # Fetch all card ids
                records = cursor.fetchall()
                # Extract ids from records. Each record is a tuple.
                card_ids = [str(record[0]) for record in records]
                return card_ids
    except Exception as e:
        print(f"An error occurred while fetching card details: {e}")
        return []


def generate_sitemap(card_ids, output_file="sitemap.xml"):
    """
    Generate an XML sitemap from card ids.
    Each URL takes the pattern: BASE_URL/<card_id>
    """
    # Create the root element with the required namespace.
    urlset = ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")

    for card_id in card_ids:
        url_elem = ET.SubElement(urlset, "url")
        loc = ET.SubElement(url_elem, "loc")
        loc.text = f"{BASE_URL}/{card_id}"
        # Optional: Add more sitemap tags as needed (e.g., <lastmod>, <changefreq>, <priority>)
        # lastmod = ET.SubElement(url_elem, "lastmod")
        # lastmod.text = "2023-01-01"  # Modify as needed

    # Build the tree and write it to file with XML declaration.
    tree = ET.ElementTree(urlset)
    tree.write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"Sitemap successfully generated at {output_file}")


if __name__ == "__main__":
    # Step 1: Get all card IDs from the database.
    card_ids = fetch_card_details()
    if not card_ids:
        print("No card records found; sitemap was not generated.")
    else:
        # Step 2: Generate the sitemap XML.
        generate_sitemap(card_ids)