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

def store_analysis_in_db(card_name, card_id, topic, analysis, temperature, related_to=None):
    """Store the analysis in the database with proper references"""
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/infinitplex_dev")
    db = client["infinityplex_dev"]
    collection = db["card_analyses"]

    # Create a document to insert with proper card_id reference
    analysis_doc = {
        "card_name": card_name,
        "card_id": card_id,  # Foreign key reference to the cards collection
        "topic": topic,
        "content": analysis,
        "temperature": temperature,
        "created_at": datetime.datetime.now()
    }

    # Add relation to previous card in chain if applicable
    if related_to:
        analysis_doc["related_to"] = related_to

    # Insert the document
    result = collection.insert_one(analysis_doc)

    return result.inserted_id

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

def find_related_card(word_counts, original_card_name, cards_collection, analyzed_cards=None):
    """
    Find a related card based on the word counts from the analysis

    Parameters:
    - word_counts: Counter object with card names and their mention counts
    - original_card_name: The name of the card that was analyzed
    - cards_collection: MongoDB collection for cards
    - analyzed_cards: List of card names already analyzed in this chain (to avoid loops)

    Returns:
    - tuple: (selected_card_name, card_document)
    """
    if not analyzed_cards:
        analyzed_cards = []

    # Remove the original card from word counts if present
    if original_card_name in word_counts:
        del word_counts[original_card_name]

    # If no mentions found
    if not word_counts:
        print("No related cards found in the analysis.")
        return None, None

    # Sort cards by mention count (highest first)
    sorted_cards = word_counts.most_common()

    # Try to find the highest count card that exists in the database
    for card_name, count in sorted_cards:
        # Skip if this card has already been analyzed in this chain
        if card_name in analyzed_cards:
            print(f"Skipping {card_name} as it was already analyzed in this chain.")
            continue

        # Check if the card exists in the database
        card = cards_collection.find_one({"name": card_name})

        if card:
            return card_name, card

    print("None of the mentioned cards found in database.")
    return None, None


def analyze_card_chain(temperature=1.0, topic="commander_deck", model="gemma3:latest", depth=2):
    """Analyze a chain of related cards, starting with a random one"""
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/infinitplex_dev")
    db = client["infinityplex_dev"]
    cards_collection = db["cards"]

    # Start with a random card
    current_depth = 1
    analyzed_cards = []
    previous_card_id = None
    previous_analysis_id = None

    # Initial random card analysis
    result = analyze_single_card(temperature, topic, model, cards_collection)
    if not result:
        return False

    original_card_name, original_card_id, original_analysis_id, word_counts = result
    analyzed_cards.append(original_card_name)
    previous_card_id = original_card_id
    previous_analysis_id = original_analysis_id

    # Continue with related cards up to the specified depth
    while current_depth < depth:
        print(f"\n{'=' * 50}")
        print(f"ANALYZING RELATED CARD (Depth {current_depth + 1}/{depth})")
        print(f"{'=' * 50}")

        # Find a related card
        related_card_name, related_card = find_related_card(word_counts, original_card_name, cards_collection)

        if not related_card:
            print("Cannot continue the analysis chain - no suitable related card found.")
            break

        # Skip if we've already analyzed this card in this chain
        if related_card_name in analyzed_cards:
            print(f"Skipping {related_card_name} as it was already analyzed in this chain.")
            break

        print(f"\nSelected related card: {related_card_name}")

        # Track the relationship to the previous card
        relation = {
            "card_id": previous_card_id,
            "analysis_id": previous_analysis_id,
            "relationship_type": "mentioned_in"
        }

        # Analyze the related card
        result = analyze_single_card(temperature, topic, model, cards_collection,
                                     related_card, relation)
        if not result:
            break

        next_card_name, next_card_id, next_analysis_id, word_counts = result
        analyzed_cards.append(next_card_name)
        original_card_name = next_card_name
        previous_card_id = next_card_id
        previous_analysis_id = next_analysis_id
        current_depth += 1

    print(f"\nCompleted analysis chain of {len(analyzed_cards)} cards: {', '.join(analyzed_cards)}")
    return True

def analyze_single_card(temperature, topic, model, cards_collection, selected_card=None, related_to=None):
    """
    Analyze a single card and return the word counts for related cards

    Parameters:
    - temperature: Temperature setting for text generation
    - topic: Analysis topic (e.g., "commander_deck")
    - model: LLM model to use
    - cards_collection: MongoDB collection for cards
    - selected_card: (Optional) A specific card document to analyze instead of random selection
    - related_to: (Optional) Information about what card/analysis this is related to

    Returns:
    - tuple: (card_name, card_id, analysis_id, word_counts)
    """
    if selected_card is None:
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
                return None
        else:
            # Get a random skip value within the count of unanalyzed cards
            random_skip = random.randint(0, unanalyzed_cards - 1)
            # Get a random unanalyzed card
            random_card = cards_collection.find(query).skip(random_skip).limit(1).next()
            print(f"Selected an unanalyzed card (from {unanalyzed_cards} available cards)")
    else:
        # Use the provided card
        random_card = selected_card

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
        return None

    print("\n=== Analysis Results ===")
    print(analysis)

    # Store the initial analysis with proper references
    analysis_id = store_analysis_in_db(card_name, card_id, topic, analysis, temperature, related_to)

    # Update the card document with analysis timestamp and last analysis reference
    current_time = datetime.datetime.now()
    cards_collection.update_one(
        {"_id": card_id},
        {"$set": {
            "card_analysis_date": current_time,
            "last_analysis_id": analysis_id
        }}
    )

    print(f"\nCard updated with analysis timestamp: {current_time}")

    # Analyze the bracket usage
    pattern = r'\[\[(.*?)\]\]'
    bracketed_terms = re.findall(pattern, analysis)
    word_counts = collections.Counter(bracketed_terms)

    print("\n=== Bracket Usage Statistics ===")
    for word, count in word_counts.most_common():
        print(f"[[{word}]]: {count}")

    # Return the card name, card ID, analysis ID, and word counts for further processing
    return card_name, card_id, analysis_id, word_counts

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
    parser.add_argument("--chain-depth", type=int, default=2,
                        help="Number of related cards to analyze in the chain (default: 2)")

    args = parser.parse_args()

    # Analyze a chain of related cards
    analyze_card_chain(args.temperature, args.topic, args.model, args.chain_depth)
    return 0


if __name__ == "__main__":
    import datetime  # Added for timestamp in database

    main()



