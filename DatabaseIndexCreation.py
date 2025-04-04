import os
import psycopg2

# Get the database connection string from an environment variable
DATABASE_URL = os.getenv("LOCAL_DB_URL")
if not DATABASE_URL:
    raise ValueError("The LOCAL_DB_URL environment variable is not set.")

# Define the SQL commands for creating indexes
sql_statements = [
    # Create an index for the "name" column.
    "CREATE INDEX IF NOT EXISTS idx_card_details_name ON card_details(name);",
    # Create an index for the "tcgplayer_id" column.
    "CREATE INDEX IF NOT EXISTS idx_card_details_tcgplayer_id ON card_details(tcgplayer_id);",
    # Create an index for the "set" column (quoted because it's a reserved word).
    'CREATE INDEX IF NOT EXISTS idx_card_details_set ON card_details("set");',
    # Create an index for the "set_name" column.
    "CREATE INDEX IF NOT EXISTS idx_card_details_set_name ON card_details(set_name);",
    # Create an index for the "artist" column.
    "CREATE INDEX IF NOT EXISTS idx_card_details_artist ON card_details(artist);",
    # Create an index for the "type_line" column.
    "CREATE INDEX IF NOT EXISTS idx_card_details_type_line ON card_details(type_line);",
    # Create an expression index for the "normal" key within the JSONB column "image_uris".
    "CREATE INDEX IF NOT EXISTS idx_card_details_image_uri_normal ON card_details ((image_uris ->> 'normal'));"
]


def create_indexes(connection_string):
    try:
        # Connect to the database
        conn = psycopg2.connect(connection_string)
        # Use autocommit to execute DDL statements without manual transaction management
        conn.autocommit = True
        cur = conn.cursor()

        for sql in sql_statements:
            print(f"Executing: {sql}")
            cur.execute(sql)

        print("Indexes created successfully.")
        cur.close()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    create_indexes(DATABASE_URL)