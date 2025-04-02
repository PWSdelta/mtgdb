import ijson
import pandas as pd
import json
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData, Table


# Database connection
engine = create_engine("postgresql+psycopg2://postgres:asdfghjkl@localhost:5432/mtgdb")


# Assuming `engine` is your SQLAlchemy engine
metadata = MetaData()
card_details_table = Table('card_details', metadata, autoload_with=engine)



def upsert_enrich_data(df, table, engine):
    IGNORE_COLUMNS = {"id", "tcgplayer_id", "normal_price"}  # Columns to ignore during updates

    # Replace NaN with None in the DataFrame for compatibility with PostgreSQL
    df = df.where(pd.notnull(df), None)

    with engine.connect() as conn:
        for _, row in df.iterrows():
            row_data = row.to_dict()

            # Get database columns and align the row data
            db_columns = {col.name for col in table.columns}
            row_data = {col: row_data.get(col, None) for col in db_columns}

            # Prepare columns for updating (exclude ignored columns)
            update_columns = {
                col: row_data[col]
                for col in db_columns
                if col not in IGNORE_COLUMNS and col in row_data
            }

            # Insert or update query with conflict resolution
            query = insert(table).values(**row_data).on_conflict_do_update(
                index_elements=["id"],  # Conflict resolution based on card ID
                set_=update_columns
            )

            # Execute the query
            conn.execute(query)


def preprocess_csv(csv_path_or_df, json_columns):
    """
    Prepares the data from the CSV (or DataFrame) for database insertion.
    - Normalizes JSON-like columns.
    - Fixes missing values (converts NaN to None for SQL compatibility).

    Args:
        csv_path_or_df: Path to the input CSV or a DataFrame.
        json_columns: Columns containing JSON objects as strings.

    Returns:
        A cleaned DataFrame ready for database insertion.
    """
    # Read the CSV if a path is provided
    if isinstance(csv_path_or_df, str):
        df = pd.read_csv(csv_path_or_df, low_memory=False, encoding='utf-8')
    else:
        df = csv_path_or_df  # Assume it's already a DataFrame

    # Replace NaN with None for SQL compatibility (applies to all columns)
    df = df.where(pd.notna(df), None)

    # Parse JSON-like columns
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(parse_json)

    # Explicitly handle 'tcgplayer_id' column (replace NaN with None)
    if 'tcgplayer_id' in df.columns:
        df['tcgplayer_id'] = df['tcgplayer_id'].apply(
            lambda x: None if isinstance(x, float) and pd.isna(x) else x
        )

    return df


def parse_json(value):
    """
    Safely parses JSON-like strings into Python objects (e.g., dictionaries).
    Args:
        value: A JSON-like string or object.
    Returns:
        A parsed object or None if invalid.
    """
    try:
        if isinstance(value, str) and value.strip():
            return json.loads(value.replace("'", '"'))  # Handle invalid JSON (e.g., single quotes)
    except json.JSONDecodeError:
        pass
    return None


def process_json_file_with_ijson(json_file_path, csv_output_path):
    """
    Processes a JSON file into a CSV file by flattening the structure.
    Uses ijson for efficient reading of large JSON files.
    Args:
        json_file_path: Path to the input JSON file.
        csv_output_path: Path to save the output CSV file.
    Returns:
        A Pandas DataFrame of the processed data.
    """
    rows = []

    # Load and process the JSON file
    with open(json_file_path, 'r', encoding='utf-8') as json_file:
        for item in ijson.items(json_file, 'item'):  # Adjust 'item' if needed to align with JSON structure
            rows.append(item)

    # Convert the rows into a DataFrame
    df = pd.DataFrame(rows)

    # Save to a CSV
    df.to_csv(csv_output_path, index=False)

    return df


def main():
    JSON_FILE_PATH = "default-cards.json"
    CSV_OUTPUT_PATH = "default-cards.csv"


    df = process_json_file_with_ijson(JSON_FILE_PATH, CSV_OUTPUT_PATH)


    json_columns = ['related_uris', 'prices', 'image_uris', 'legalities']
    print(f"Processing {JSON_FILE_PATH}. Soon, {CSV_OUTPUT_PATH} will be ready.")
    cleaned_df = preprocess_csv(CSV_OUTPUT_PATH, json_columns)
    print(f"{CSV_OUTPUT_PATH} is ready!")


    cleaned_df['tcgplayer_id'] = cleaned_df['tcgplayer_id'].where(
        pd.notnull(cleaned_df['tcgplayer_id']), None
    )

    cleaned_df = cleaned_df.drop_duplicates(subset=['tcgplayer_id'], keep='first')

    print(f"Processing {CSV_OUTPUT_PATH}. Fingers crossed...")
    upsert_enrich_data(cleaned_df, card_details_table, engine)
    print("Done!")


if __name__ == "__main__":
    main()