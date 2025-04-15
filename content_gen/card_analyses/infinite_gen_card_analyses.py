import os

import pymongo
import argparse
import random
import re
import collections


import requests


# client = pymongo.MongoClient("mongodb://localhost:27017")
client = pymongo.MongoClient('mongodb+srv://pwsdelta_user:asdfghjkl@cluster0.8uviyvr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client["mtgdbmongo"]
cards_collection = db["cards"]


def generate_llm_response(prompt, temperature=0.919, model="llama3:latest"):
    """Generate a response using a local Ollama LLM"""
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "temperature": temperature,
                "stream": False
            },
            timeout=27
        )

        if response.status_code == 200:
            return response.json().get("response", "")
        else:
            print(f"Error: Received status code {response.status_code} from Ollama API")
            return ""
    except Exception as e:
        print(f"Error generating LLM response: {e}")
        return ""

def store_analysis_in_db(card_name, card_id, analysis, card):
    """Store the analysis in the database with proper references"""
    collection = db["card_analyses"]

    # Create a document to insert/update with proper card_id reference
    analysis_doc = {
        "card_name": card_name,
        "card_id": card["id"],  # Using dictionary access
        "content": analysis,
        "created_at": datetime.datetime.now(),
        "updated_at": datetime.datetime.now()
    }

    # Use update_one with upsert=True to update an existing record or create a new one
    result = collection.update_one(
        {"card_id": card["id"]},  # Use dictionary access here too
        {"$set": analysis_doc},
        upsert=True
    )

    # Return the _id - either of the updated document or the newly inserted one
    if result.upserted_id:
        return result.upserted_id
    else:
        # If we updated an existing document, find it to return its _id
        updated_doc = collection.find_one({"card_id": card["id"]})  # Again, dictionary access
        return updated_doc["_id"] if updated_doc else None

def analyze_cards_in_order(temperature=0.919, topic="commander_deck", model="gemma3:1b"):
    """
    Analyze cards sequentially in the order they appear in the database.

    Parameters:
    - temperature: Temperature setting for text generation
    - topic: Analysis topic (e.g., "commander_deck")
    - model: LLM model to use

    Returns:
    - bool: Success status
    """
    # Get all cards that haven't been analyzed yet
    unanalyzed_query = {"card_analysis_date": {"$exists": False}}
    all_cards_cursor = cards_collection.find(unanalyzed_query)

    print(f"\n{'=' * 50}")
    print(f"STARTING SEQUENTIAL CARD ANALYSIS")
    print(f"{'=' * 50}")

    try:
        for card_doc in all_cards_cursor:
            # Extract card details
            card_name = card_doc.get("name", "Unknown Card")
            card_id = card_doc.get("_id")

            print(f"\n{'=' * 50}")
            print(f"Card: {card_name}")
            print(f"{'=' * 50}")

            # Generate the analysis
            print(f"\nGenerating analysis for {card_name}...")

            prompt = f"""
            Analyze the Magic: The Gathering card '{card_name}'.

            Include these sections with ONLY bolded headers and NO numbering. Please use full sentences and generate a healthy amount of content. Please also be sure to mention other cards wherever you can if it makes sense in context with the current card:

            **Power Level and Overview**

            **Common Strategies**

            **Budget & Progression Options**

            **Off-Meta Interactions**

            **Meta Position**

            **Deck Building**

            **Combo Potential**

            **Budget Considerations**

            **Technical Play**

            **Card Interactions**

            **Legality & Historical Rulings**
            
            **Five Cards Everyone Should Know**

            CRITICAL: DO NOT INCLUDE any of the following in your response:
            - DO NOT include any section numbers
            - DO NOT include any dividing lines (---)
            - DO NOT include the phrase "Common Follow-up Categories:"
            - DO NOT include any ## headings

            Just provide a continuous analysis with simple bolded section headers only.

            """

            analysis = generate_llm_response(prompt, temperature=temperature, model=model)

            if not analysis:
                print(f"Failed to analyze '{card_name}'. Continuing with next card.")
                continue

            print("\n=== Analysis Results ===")
            print(analysis)

            # Store the analysis
            analysis_id = store_analysis_in_db(card_name, card_id, analysis, card_doc)
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Saving progress...")
    except Exception as e:
        print(f"\n\nError encountered: {str(e)}")

    return True


def main():
    analyze_cards_in_order(
        temperature=0.919,
        topic="commander_format",
        model="gemma3:1b"
    )

    return 0

if __name__ == "__main__":
    import datetime  # Added for timestamp in database

    main()



