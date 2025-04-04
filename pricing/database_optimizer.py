# Language: Python

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from your .env file.
load_dotenv()

# Use your connection string for local vs production as needed.
DB_CONNECTION_STRING = os.environ.get('RW_DATABASE_URL')


def optimize_db(connection_string, table_name="card_details"):
    """
    Optimize the PostgreSQL database table by performing:
      - VACUUM ANALYZE: Updates planner statistics and cleans up dead rows.
      - REINDEX TABLE: Rebuilds indexes on the table.

    VACUUM and REINDEX require autocommit mode, which is set upon connection.
    """
    try:
        # Open the connection and set autocommit before creating any cursors.
        conn = psycopg2.connect(connection_string)
        conn.autocommit = True

        cursor = conn.cursor()

        # Run VACUUM ANALYZE (must be run with autocommit enabled).
        print(f"Running VACUUM ANALYZE on {table_name}...")
        cursor.execute(f"VACUUM ANALYZE {table_name};")
        print(f"VACUUM ANALYZE completed on {table_name}.")

        # Run REINDEX TABLE.
        print(f"Running REINDEX on {table_name}...")
        cursor.execute(f"REINDEX TABLE {table_name};")
        print(f"REINDEX completed on {table_name}.")

        cursor.close()
        conn.close()
        print("Database optimization tasks completed successfully.")
    except Exception as e:
        print(f"An error occurred during database optimization: {e}")


if __name__ == "__main__":
    optimize_db(DB_CONNECTION_STRING)