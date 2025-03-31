import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values




def get_existing_columns(cursor, table_name):
    """
    Fetch the existing columns from the given database table.
    """
    cursor.execute(f"""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = '{table_name}' AND table_schema = 'public';
    """)
    return [row[0] for row in cursor.fetchall()]


def add_missing_columns_to_table(cursor, table_name, dataframe, existing_columns):
    """
    Dynamically add missing columns from the CSV DataFrame to the database table.
    """
    for column in dataframe.columns:
        if column not in existing_columns:
            print(f"Column '{column}' is missing in table '{table_name}'. Adding it...")
            alter_query = f'ALTER TABLE {table_name} ADD COLUMN "{column}" TEXT;'
            cursor.execute(alter_query)
            existing_columns.append(column)


def auto_map_and_insert(file_path, table_name, connection):
    """
    Automatically maps CSV file columns to the database table and inserts records.
    """
    try:
        print(f"Processing file: {file_path}")

        # Step 1: Load the CSV file
        df = pd.read_csv(file_path)

        # Step 2: Validate the presence of the `productId` column
        if "productId" not in df.columns:
            raise ValueError(f"The `productId` column is missing in the file: {file_path}")

        # Step 3: Establish a cursor and fetch schema details
        cursor = connection.cursor()
        existing_columns = get_existing_columns(cursor, table_name)

        # Step 4: Dynamically add missing columns to the database table
        # Excluding `id` (auto-increment managed by the database)
        add_missing_columns_to_table(cursor, table_name, df, existing_columns)

        # Step 5: Align DataFrame columns with the database table, excluding `id`
        if "id" in df.columns:
            df = df.drop(columns=["id"])  # Drop `id` if it somehow exists

        for col in existing_columns:
            if col != "id" and col not in df.columns:  # Exclude `id`
                # print(f"Missing column '{col}' in the file. Filling with `None`...")
                df[col] = None

        # Align DataFrame columns with table schema excluding `id`
        aligned_df = df[[col for col in existing_columns if col != "id"]]

        # Debugging: Print first few rows of the aligned DataFrame
        # print(f"Aligned Columns: {aligned_df.columns.tolist()}")
        # print(f"Data preview:\n{aligned_df.head()}")

        # Step 6: Prepare and execute the insertion query
        column_names = ', '.join([f'"{col}"' for col in aligned_df.columns])
        insert_query = f"""
        INSERT INTO {table_name} ({column_names})
        VALUES %s
        ON CONFLICT DO NOTHING;
        """

        # Convert DataFrame rows to tuples for insertion
        rows = aligned_df.itertuples(index=False, name=None)

        # Bulk execute the insertion
        execute_values(cursor, insert_query, rows)

        # Commit the transaction
        connection.commit()
        print(f"Successfully inserted data from {file_path} into '{table_name}'.")

    except Exception as e:
        print(f"Error processing file '{file_path}': {e}")
        if connection:
            connection.rollback()


def main():
    # Database connection details
    db_params = {
        "dbname": "mtgdb",
        "user": "postgres",
        "password": "asdfghjkl",
        "host": "localhost",
        "port": 5432
    }

    # Folder containing the CSV files
    csv_folder = "downloads/"

    # The table to insert data into
    table_name = "products"

    # Connect to the database
    try:
        connection = psycopg2.connect(**db_params)
        connection.autocommit = True
        print("Successfully connected to the database.")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return

    # List all files in the CSV directory
    all_files = [os.path.join(csv_folder, file) for file in os.listdir(csv_folder) if file.endswith(".csv")]
    print(f"Files found for processing: {all_files}")

    # Process each filePERFECT_UpdateALLProductsFromCSVFolder.py
    for file_path in all_files:
        auto_map_and_insert(file_path, table_name, connection)

    # Close the database connection
    connection.close()
    print("Database connection closed.")


if __name__ == "__main__":
    main()