import psycopg2
import time

# Database configuration - update with your actual credentials
DB_CONFIG = {
    'host': 'localhost',
    'database': 'mtgdb',  # Update with your database name
    'user': 'postgres',
    'password': 'asdfghjkl',  # Update with your actual password
    'port': '5432'
}


def run_database_operations():
    """
    Run three sequential database queries in a loop from 1 to 100
    """
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Start the loop from 1 to 100
        for i in range(1, 101):
            print(f"Processing iteration {i}/100...")

            # First database operation
            query1 = """
            SELECT COUNT(*) FROM card_details 
            WHERE id % 100 = %s;
            """
            cursor.execute(query1, (i % 100,))
            result1 = cursor.fetchone()
            print(f"  Query 1 result: {result1[0]} cards found")

            # Second database operation
            query2 = """
            SELECT name, set_name FROM card_details 
            WHERE id % 100 = %s
            LIMIT 1;
            """
            cursor.execute(query2, (i % 100,))
            result2 = cursor.fetchone()
            if result2:
                print(f"  Query 2 result: Card: {result2[0]}, Set: {result2[1]}")
            else:
                print("  Query 2 result: No card found")

            # Third database operation
            query3 = """
            UPDATE card_details 
            SET last_checked = CURRENT_TIMESTAMP 
            WHERE id % 100 = %s
            RETURNING id;
            """
            cursor.execute(query3, (i % 100,))
            affected_rows = cursor.rowcount
            conn.commit()
            print(f"  Query 3 result: Updated {affected_rows} rows")

            # Optional: small delay between iterations to avoid overwhelming the database
            time.sleep(0.1)

        # Close database connection
        cursor.close()
        conn.close()
        print("Database operations completed successfully!")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        # If connection was established, close it
        if 'conn' in locals() and conn is not None:
            conn.close()


if __name__ == "__main__":
    run_database_operations()