from flask import request, jsonify
import json
import os
from sqlalchemy import Table, Column, Integer, JSON, MetaData, String
import logging



# Assuming these are already defined in your project
# app, engine, Session already exist

@app.route('/api/import-json-folder', methods=['POST'])
def import_json_folder():
    """
    Endpoint to import all JSON files from a specified folder into PostgreSQL database.
    Each JSON file will be stored as a complete JSONB document in a single column.
    """
    # data = 'pricing/AllDeckFiles'
    # if not data or 'folder_path' not in data:
    #     return jsonify({"error": "No folder path provided"}), 400

    folder_path = 'pricing/AllDeckFiles/'

    if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
        return jsonify({"error": "Invalid folder path"}), 400

    # Start a session
    session = Session()
    try:
        # Define a simple table with id, filename, and document columns
        metadata = MetaData()
        json_documents = Table(
            'json_documents',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('filename', String),  # Store the original filename
            Column('document', JSON)  # This will be stored as JSONB in PostgreSQL
        )

        # Create the table if it doesn't exist
        metadata.create_all(engine)

        total_files = 0
        processed_files = 0
        errors = []

        # Process each JSON file in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith('.json'):
                total_files += 1
                file_path = os.path.join(folder_path, filename)

                try:
                    # Load the JSON data
                    with open(file_path, 'r', encoding='utf-8') as file:
                        json_data = json.load(file)

                    # Insert the entire document as JSONB
                    session.execute(json_documents.insert().values(
                        filename=filename,
                        document=json_data
                    ))

                    processed_files += 1

                    # Commit every few files to avoid large transactions
                    if processed_files % 50 == 0:
                        session.commit()

                except json.JSONDecodeError as e:
                    errors.append(f"Invalid JSON in {filename}: {str(e)}")
                    logging.error(f"Error parsing {filename}: {str(e)}")
                except Exception as e:
                    errors.append(f"Error processing {filename}: {str(e)}")
                    logging.error(f"Error processing {filename}: {str(e)}")

        # Final commit for remaining files
        session.commit()

        return jsonify({
            "status": "success" if not errors else "partial success",
            "message": f"Processed {processed_files} of {total_files} JSON files",
            "processed_count": processed_files,
            "total_count": total_files,
            "errors": errors
        }), 200 if not errors else 207  # 207 Multi-Status

    except Exception as e:
        session.rollback()
        logging.error(f"Error during import: {str(e)}")
        return jsonify({"error": f"Error during import: {str(e)}"}), 500
    finally:
        session.close()


# Alternative implementation using command-line arguments
# This can be called directly from a script instead of as an endpoint
def import_json_files_from_folder(folder_path):
    """
    Function to import all JSON files from a specified folder into PostgreSQL database.
    This can be used in a command-line script or scheduled task.
    """
    if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
        print(f"Error: Invalid folder path {folder_path}")
        return

    # Create a session
    session = Session()
    try:
        # Define the table with the same structure as above
        metadata = MetaData()
        json_documents = Table(
            'json_documents',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('filename', String),
            Column('document', JSON)
        )

        # Create the table if it doesn't exist
        metadata.create_all(engine)

        total_files = 0
        processed_files = 0

        print(f"Starting import from {folder_path}")

        # Process each JSON file in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith('.json'):
                total_files += 1
                file_path = os.path.join(folder_path, filename)

                try:
                    print(f"Processing {filename}...")

                    # Load the JSON data
                    with open(file_path, 'r', encoding='utf-8') as file:
                        json_data = json.load(file)

                    # Insert the entire document as JSONB
                    session.execute(json_documents.insert().values(
                        filename=filename,
                        document=json_data
                    ))

                    processed_files += 1

                    # Commit every few files
                    if processed_files % 50 == 0:
                        session.commit()
                        print(f"Committed {processed_files} files so far...")

                except Exception as e:
                    print(f"Error processing {filename}: {str(e)}")
                    logging.error(f"Error processing {filename}: {str(e)}")

        # Final commit
        session.commit()

        print(f"Import complete. Processed {processed_files} of {total_files} JSON files")

    except Exception as e:
        session.rollback()
        print(f"Error during import: {str(e)}")
        logging.error(f"Error during import: {str(e)}")
    finally:
        session.close()


# Example usage as a command-line script
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python script_name.py /path/to/json/folder")
    else:
        folder_path = sys.argv[1]
        import_json_files_from_folder(folder_path)