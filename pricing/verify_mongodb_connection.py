import os
import sys
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


def verify_mongodb_connection(connection_string):
    """
    Connects to MongoDB using the provided connection string and verifies the connection.
    
    Args:
        connection_string (str): MongoDB connection string
        
    Returns:
        bool: True if connection is successful, False otherwise
    """
    print(f"Attempting to connect to MongoDB...")

    try:
        # Connect with a short timeout
        client = MongoClient(connection_string)

        # Force a command to verify the connection is working
        client.admin.command('ping')

        # Get server info
        server_info = client.server_info()

        print("\n✅ Connection Successful!")
        print(f"MongoDB version: {server_info.get('version')}")

        # List available databases
        database_names = client.list_database_names()
        print(f"\nAvailable databases ({len(database_names)}):")
        for db_name in database_names:
            db = client[db_name]
            collection_count = len(db.list_collection_names())
            print(f"  - {db_name} ({collection_count} collections)")

        # Additional details about the connection
        print("\nConnection details:")
        print(f"  - Host: {client.address[0]}")
        print(f"  - Port: {client.address[1]}")

        return True

    except ConnectionFailure as e:
        print(f"\n❌ Connection Failed: {e}")
        return False

    except ServerSelectionTimeoutError as e:
        print(f"\n❌ Server Selection Timeout: {e}")
        print("This usually means the server is unreachable or not running.")
        return False

    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        return False

    finally:
        if 'client' in locals():
            client.close()
            print("\nConnection closed.")


if __name__ == "__main__":
    # Get connection string from command line or use default
    connection_string = ''

    # Verify connection
    success = verify_mongodb_connection(connection_string)

    # Exit with appropriate code
    sys.exit(0 if success else 1)