import pymongo
import argparse
import random
import re
import collections

import requests


def generate_llm_response(prompt, temperature=1.0, model="llama3:latest"):
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
            timeout=451
        )

        if response.status_code == 200:
            return response.json().get("response", "")
        else:
            print(f"Error: Received status code {response.status_code} from Ollama API")
            return ""
    except Exception as e:
        print(f"Error generating LLM response: {e}")
        return ""

def store_analysis_in_db(card_name, topic, analysis, temperature):
    """Store the analysis in the database"""
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/infinitplex_dev")
    db = client["infinityplex_dev"]
    collection = db["card_analyses"]

    # Create a document to insert
    analysis_doc = {
        "card_name": card_name,
        "topic": topic,
        "content": analysis,
        "temperature": temperature,
        "created_at": datetime.datetime.now()
    }

    # Insert the document
    collection.insert_one(analysis_doc)


def analyze_random_card(temperature=1.0, topic="commander_deck", model="gemma3:latest"):
    """Analyze a random Magic: The Gathering card from the database that hasn't been analyzed yet"""
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/infinitplex_dev")
    db = client["infinityplex_dev"]
    cards_collection = db["cards"]

    # Create a filter for cards that don't have an analysis date
    query = {"card_analysis_date": {"$exists": False}}

    # Count total unanalyzed cards
    unanalyzed_cards = cards_collection.count_documents(query)

    if unanalyzed_cards == 0:
        print("No unanalyzed cards found in the database.")
        reanalyze = input("Would you like to analyze a previously analyzed card? (y/n): ").strip().lower()
        if reanalyze == 'y':
            # If all cards have been analyzed, pick a random one regardless of analysis date
            total_cards = cards_collection.count_documents({})
            random_skip = random.randint(0, total_cards - 1)
            random_card = cards_collection.find().skip(random_skip).limit(1).next()
            print("Selecting a previously analyzed card...")
        else:
            print("Exiting without analysis.")
            return False
    else:
        # Get a random skip value within the count of unanalyzed cards
        random_skip = random.randint(0, unanalyzed_cards - 1)
        # Get a random unanalyzed card
        random_card = cards_collection.find(query).skip(random_skip).limit(1).next()
        print(f"Selected an unanalyzed card (from {unanalyzed_cards} available cards)")

    # Extract the card name and id
    card_name = random_card.get("name", "Unknown Card")
    card_id = random_card.get("_id")

    # Check if this card has been analyzed before
    previously_analyzed = "card_analysis_date" in random_card
    if previously_analyzed:
        last_analysis = random_card.get("card_analysis_date")
        print(f"Note: This card was previously analyzed on {last_analysis}")

    print(f"\n=== Analyzing Card: {card_name} ===")
    print(f"Temperature: {temperature}")
    print(f"Topic: {topic}")
    print(f"Model: {model}")

    # Generate the initial comprehensive analysis
    print(f"\nGenerating comprehensive analysis for {card_name}...")

    prompt = f"""
    Analyze the Magic: The Gathering card '{card_name}' for {topic}.

    Include these sections with ONLY bolded headers and NO numbering. Please use full sentences and generate a healthy amount of content. Please also be sure to mention other cards wherever you can if it makes sense in context with the current card:

    **Power Level and Overview**

    **Common Strategies**

    **Budget & Progression Options**

    **Off-Meta Interactions**

    **Meta Position**

    **Deck Building**

    **Meta Positioning**

    **Combo Potential**

    **Budget Considerations**

    **Technical Play**

    **Card Interactions**

    **Legality & Historical Rulings**

    CRITICAL: DO NOT INCLUDE any of the following in your response:
    - DO NOT include any section numbers
    - DO NOT include any dividing lines (---)
    - DO NOT include the phrase "Common Follow-up Categories:"
    - DO NOT include any ## headings

    Just provide a continuous analysis with simple bolded section headers only.

    In your response, wrap the names of real Magic: The Gathering cards with double brackets like this: [[Card Name]].
    """

    analysis = generate_llm_response(prompt, temperature=temperature, model=model)

    if not analysis:
        return False

    print("\n=== Analysis Results ===")
    print(analysis)

    # Store the initial analysis
    store_analysis_in_db(card_name, topic, analysis, temperature)

    # Update the card document with analysis timestamp
    current_time = datetime.datetime.now()
    cards_collection.update_one(
        {"_id": card_id},
        {"$set": {"card_analysis_date": current_time}}
    )

    print(f"\nCard updated with analysis timestamp: {current_time}")

    # Analyze the bracket usage
    pattern = r'\[\[(.*?)\]\]'
    bracketed_terms = re.findall(pattern, analysis)
    word_counts = collections.Counter(bracketed_terms)

    print("\n=== Bracket Usage Statistics ===")
    for word, count in word_counts.most_common():
        print(f"[[{word}]]: {count}")

    print("\nComprehensive analysis completed and saved to database.")
    return True


def main():
    # Parse command line arguments for options
    parser = argparse.ArgumentParser(description="MTG Random Card Analysis Tool")

    # Optional arguments with defaults
    parser.add_argument("--temperature", type=float, default=1.0,
                        help="Temperature setting for text generation (default: 1.0)")
    parser.add_argument("--topic", type=str, default="commander_deck",
                        help="Analysis topic (default: commander_deck)")
    parser.add_argument("--model", type=str, default="gemma3:latest",
                        help="Ollama model to use (default: gemma3:latest)")

    args = parser.parse_args()

    # Analyze a random card
    analyze_random_card(args.temperature, args.topic, args.model)
    return 0


if __name__ == "__main__":
    import datetime  # Added for timestamp in database

    main()