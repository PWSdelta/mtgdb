# Language: Python

import psycopg2
import psycopg2.extras
import json
import os
from dotenv import load_dotenv
import time
import csv

# Load environment variables
load_dotenv()

DB_CONNECTION_STRING = os.environ.get('RW_DATABASE_URL')


def normalize_list(value):
    """Return a sorted tuple for list values or the value itself if not a list."""
    if isinstance(value, list):
        return tuple(sorted(value))
    return value


def get_composite_key(item):
    """
    Create a composite key for duplicate detection using:
    - tcgplayer_id (as string)
    - illustration_id
    - normalized artist_ids (sorted tuple if list; otherwise the value)
    """
    tcgplayer_id = item.get('tcgplayer_id')
    if tcgplayer_id is not None:
        tcgplayer_id = str(tcgplayer_id)
    illustration_id = item.get('illustration_id')
    artist_ids = normalize_list(item.get('artist_ids'))
    return (tcgplayer_id, illustration_id, artist_ids)


def remove_constraint(cursor, table_name, constraint_name):
    """
    Attempt to remove the specified constraint if present.
    In production, permissions may be restricted so errors are caught.
    """
    try:
        print(f"Attempting to drop constraint '{constraint_name}' on table '{table_name}' if it exists...")
        drop_query = f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name};"
        cursor.execute(drop_query)
        print(f"Constraint '{constraint_name}' was removed (if it existed).")
    except Exception as e:
        print(
            f"Warning: Could not drop constraint '{constraint_name}'. Possibly due to permission restrictions. Error: {e}")


def process_cards(json_file_path, table_name="card_details", batch_size=10000):
    try:
        print(f"Loading JSON file {json_file_path}...")
        start_time = time.time()

        # Read the JSON file
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        print(f"JSON file loaded in {time.time() - start_time:.2f} seconds. Contains {len(data)} records.")

        # Create a file to log duplicate tcgplayer entries
        duplicate_log_file = 'tcgplayer_id_duplicates.csv'
        with open(duplicate_log_file, 'w', newline='', encoding='utf-8') as log_file:
            log_writer = csv.writer(log_file)
            log_writer.writerow([
                'tcgplayer_id', 'id', 'name', 'set', 'collector_number', 'set_name', 'lang',
                'existing_lang', 'illustration_id', 'artist_ids', 'existing_illustration_id', 'existing_artist_ids'
            ])

            # Connect to the PostgreSQL database
            with psycopg2.connect(DB_CONNECTION_STRING) as conn:
                with conn.cursor() as cursor:
                    # Attempt to remove multiple constraints if they exist.
                    remove_constraint(cursor, table_name, "card_details_unique_constraint")
                    remove_constraint(cursor, table_name, "card_details_tcgplayer_id_key")
                    conn.commit()

                    # Get the table columns from the database schema
                    cursor.execute(f"""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}'
                    """)
                    columns_info = cursor.fetchall()
                    column_names = [col[0] for col in columns_info]
                    print(f"Found {len(column_names)} columns in table '{table_name}'")

                    # Fetch existing IDs to avoid re-insert
                    cursor.execute(f"SELECT id FROM {table_name};")
                    existing_ids = {str(row[0]) for row in cursor.fetchall()}
                    print(f"Fetched {len(existing_ids)} existing IDs from the database.")

                    # Fetch existing records for duplicate detection by composite key
                    cursor.execute(f"""
                        SELECT tcgplayer_id, id, name, set, collector_number, set_name, lang, artist_ids, illustration_id
                        FROM {table_name}
                        WHERE tcgplayer_id IS NOT NULL;
                    """)
                    rows = cursor.fetchall()
                    existing_composite = {}
                    for row in rows:
                        record = {
                            'tcgplayer_id': str(row[0]) if row[0] is not None else None,
                            'id': row[1],
                            'name': row[2],
                            'set': row[3],
                            'collector_number': row[4],
                            'set_name': row[5],
                            'lang': row[6],
                            'artist_ids': row[7],
                            'illustration_id': row[8]
                        }
                        key = get_composite_key(record)
                        existing_composite[key] = record

                    print(f"Fetched {len(existing_composite)} existing composite records from the database.")

                    duplicate_by_language = {}
                    language_counts = {'same_lang': 0, 'diff_lang': 0}

                    total_inserted = 0
                    total_skipped = 0
                    total_duplicates = 0

                    # Process in batches
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        new_records = []

                        for item in batch:
                            item_id = str(item.get('id', ''))
                            if not item_id or item_id in existing_ids:
                                total_skipped += 1
                                continue

                            tcgplayer_id = item.get('tcgplayer_id')
                            if tcgplayer_id is not None:
                                composite_key = get_composite_key(item)
                                if composite_key in existing_composite:
                                    total_duplicates += 1

                                    card_lang = item.get('lang', 'Unknown')
                                    existing_lang = existing_composite[composite_key].get('lang', 'Unknown')
                                    if card_lang == existing_lang:
                                        language_counts['same_lang'] += 1
                                    else:
                                        language_counts['diff_lang'] += 1
                                        lang_pair = f"{card_lang} vs {existing_lang}"
                                        duplicate_by_language[lang_pair] = duplicate_by_language.get(lang_pair, 0) + 1

                                    log_writer.writerow([
                                        tcgplayer_id,
                                        item_id,
                                        item.get('name', 'Unknown'),
                                        item.get('set', 'Unknown'),
                                        item.get('collector_number', 'Unknown'),
                                        item.get('set_name', 'Unknown'),
                                        card_lang,
                                        existing_lang,
                                        item.get('illustration_id', 'Unknown'),
                                        item.get('artist_ids', 'Unknown'),
                                        existing_composite[composite_key].get('illustration_id', 'Unknown'),
                                        existing_composite[composite_key].get('artist_ids', 'Unknown')
                                    ])
                                    continue

                                existing_composite[composite_key] = item

                            new_records.append(item)

                        if not new_records:
                            print(
                                f"Batch {i // batch_size + 1}: No new records to insert. Total skipped: {total_skipped}")
                            continue

                        # Map JSON fields to DB columns
                        insert_records = []
                        for item in new_records:
                            record = {}
                            for col_name in column_names:
                                if col_name in item:
                                    record[col_name] = item[col_name]
                                else:
                                    parts = col_name.split('_')
                                    camel_case = parts[0] + ''.join(word.capitalize() for word in parts[1:])
                                    if camel_case in item:
                                        record[col_name] = item[camel_case]
                                    else:
                                        pascal_case = ''.join(word.capitalize() for word in col_name.split('_'))
                                        if pascal_case in item:
                                            record[col_name] = item[pascal_case]
                            if record and 'id' in record:
                                insert_records.append(record)

                        common_columns = [col for col in column_names if
                                          any(col in record for record in insert_records)]
                        columns_str = ', '.join(f'"{col}"' for col in common_columns)
                        placeholders_str = ', '.join(['%s'] * len(common_columns))
                        insert_query = f'INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders_str});'

                        values_list = []
                        for record in insert_records:
                            row_values = []
                            for col in common_columns:
                                value = record.get(col)
                                if isinstance(value, (dict, list)):
                                    value = json.dumps(value)
                                row_values.append(value)
                            values_list.append(row_values)

                        if i == 0 and values_list:
                            print(f"Sample insert query: {insert_query}")
                            print(f"First row has {len(values_list[0])} values for {len(common_columns)} columns")

                        if values_list:
                            psycopg2.extras.execute_batch(cursor, insert_query, values_list)
                            conn.commit()
                            for record in insert_records:
                                existing_ids.add(str(record['id']))

                            batch_inserted = len(values_list)
                            total_inserted += batch_inserted
                            print(f"Batch {i // batch_size + 1}: Inserted {batch_inserted} records. "
                                  f"Total: {total_inserted}, Skipped: {total_skipped}, Duplicates: {total_duplicates}")

                    print(f"\nImport completed:")
                    print(f"- Total records inserted: {total_inserted}")
                    print(f"- Total records skipped (existing ID): {total_skipped}")
                    print(f"- Total duplicate records (by composite key): {total_duplicates}")
                    print(f"- Total time: {time.time() - start_time:.2f} seconds")

                    print(f"\nLanguage analysis for duplicate tcgplayer_ids:")
                    if total_duplicates > 0:
                        same_pct = language_counts['same_lang'] / total_duplicates * 100
                        diff_pct = language_counts['diff_lang'] / total_duplicates * 100
                        print(f"- Same language duplicates: {language_counts['same_lang']} ({same_pct:.1f}%)")
                        print(f"- Different language duplicates: {language_counts['diff_lang']} ({diff_pct:.1f}%)")
                    else:
                        print("No duplicate language analysis available.")

                    if language_counts['diff_lang'] > 0:
                        print("\nDuplicates by language pairs:")
                        for lang_pair, count in sorted(duplicate_by_language.items(), key=lambda x: -x[1]):
                            print(f"  {lang_pair}: {count} duplicates")

                    print(f"\nDuplicate tcgplayer_id analysis:")
                    print(f"- Details saved to: {duplicate_log_file}")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    json_file_path = 'all-cards.json'  # Update with your actual JSON file path
    process_cards(json_file_path)