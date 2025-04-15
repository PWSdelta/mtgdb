import pymongo
import argparse
import random
import re
import collections


import requests


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
            timeout=51
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
    db = client["mtgdbmongo"]
    collection = db["card_analyses"]

    # Create a document to insert/update with proper card_id reference
    analysis_doc = {
        "card_name": card_name,
        "topic": topic,
        "content": analysis,
        "temperature": temperature,
        "created_at": datetime.datetime.now(),
        "updated_at": datetime.datetime.now()  # Add an updated_at timestamp
    }

    # Add relation to previous card in chain if applicable
    if related_to:
        analysis_doc["related_to"] = related_to

    # Use update_one with upsert=True to update an existing record or create a new one
    # We use card_id as the unique identifier to determine if the record exists
    result = collection.update_one(
        {"card_id": card_id},  # Filter by card_id
        {"$set": analysis_doc},  # Update with the new document
        upsert=True  # Create new document if no matching document exists
    )

    # Return the _id - either of the updated document or the newly inserted one
    if result.upserted_id:
        return result.upserted_id
    else:
        # If we updated an existing document, find it to return its _id
        updated_doc = collection.find_one({"card_id": card_id})
        return updated_doc["_id"] if updated_doc else None

def analyze_random_card(temperature=0.919, topic="commander_deck", model="gemma3:1b"):
    """Analyze a random Magic: The Gathering card from the database that hasn't been analyzed yet"""
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/infinitplex_dev")
    db = client["mtgdbmongo"]
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
    # previously_analyzed = "card_analysis_date" in random_card
    # if previously_analyzed:
    #     last_analysis = random_card.get("card_analysis_date")
    #     print(f"Note: This card was previously analyzed on {last_analysis}")

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

def analyze_card_chain(temperature=0.919, topic="commander_deck", model="gemma3:1b", depth=2):
    """Analyze a chain of related cards, starting with a random one"""
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/infinitplex_dev")
    db = client["mtgdbmongo"]
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
    
    **Five Cards Everyone Should Know***

    CRITICAL: DO NOT INCLUDE any of the following in your response:
    - DO NOT include any section numbers
    - DO NOT include any dividing lines (---)
    - DO NOT include the phrase "Common Follow-up Categories:"
    - DO NOT include any ## headings

    Just provide a continuous analysis with simple bolded section headers only.
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

    # Return the card name, card ID, analysis ID, and word counts for further processing
    return card_name, card_id, analysis_id

def analyze_card_queue(temperature=0.919, topic="commander_deck", model="gemma3:1b", max_cards=None,
                       continue_when_empty=True):
    """
    Analyze a queue of cards sequentially, starting with a random one
    and prioritizing based on mention count. When queue is empty, select a random card.

    Parameters:
    - temperature: Temperature setting for text generation
    - topic: Analysis topic (e.g., "commander_deck")
    - model: LLM model to use
    - max_cards: Maximum number of cards to analyze (None for unlimited)
    - continue_when_empty: If True, select a random card when queue becomes empty

    Returns:
    - bool: Success status
    """
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["mtgdbmongo"]
    cards_collection = db["cards"]

    # Sets to track processed and queued cards to avoid duplicates
    processed_cards = set()
    card_queue = []  # List of (card_name, related_to, priority) tuples

    # Track relationships for the chain
    previous_card_id = None
    previous_analysis_id = None

    # Start with a random card
    print(f"\n{'=' * 50}")
    print(f"STARTING CARD ANALYSIS QUEUE")
    if max_cards:
        print(f"Maximum cards to process: {max_cards}")
    else:
        print("Processing entire queue (no maximum limit)")
    print(f"{'=' * 50}")

    # Initial random card analysis
    result = analyze_single_card(temperature, topic, model, cards_collection)
    if not result:
        return False

    first_card_name, first_card_id, first_analysis_id, word_counts = result
    processed_cards.add(first_card_name)
    previous_card_id = first_card_id
    previous_analysis_id = first_analysis_id

    # Add mentioned cards to the queue with their priority (mention count)
    for card_name, count in word_counts.most_common():
        if card_name != first_card_name and card_name not in processed_cards:
            relation = {
                "card_id": previous_card_id,
                "analysis_id": previous_analysis_id,
                "relationship_type": "mentioned_in",
                "mention_count": count
            }
            card_queue.append((card_name, relation, count))

    # Display initial queue
    print("\nInitial card queue:")
    for i, (card_name, _, priority) in enumerate(card_queue[:10]):
        print(f"{i + 1}. {card_name} (priority: {priority})")
    if len(card_queue) > 10:
        print(f"...and {len(card_queue) - 10} more")

    # Process the queue sequentially
    cards_analyzed = 1  # Count the first card

    try:
        while True:  # Changed to infinite loop, will exit based on max_cards or KeyboardInterrupt
            # Check if we've reached the maximum (if specified)
            if max_cards and cards_analyzed >= max_cards:
                print(f"\nReached maximum card limit ({max_cards}). Stopping.")
                break

            # Check if queue is empty and handle accordingly
            if not card_queue:
                if not continue_when_empty:
                    print("\nQueue is empty. Stopping analysis.")
                    break

                print(f"\n{'=' * 50}")
                print("QUEUE IS EMPTY - SELECTING A RANDOM CARD TO CONTINUE")
                print(f"{'=' * 50}")

                # Get a random card that hasn't been processed yet
                unprocessed_query = {"name": {"$nin": list(processed_cards)}}
                unprocessed_count = cards_collection.count_documents(unprocessed_query)

                if unprocessed_count > 0:
                    # There are still unanalyzed cards
                    random_skip = random.randint(0, unprocessed_count - 1)
                    random_card = cards_collection.find(unprocessed_query).skip(random_skip).limit(1).next()
                    next_card_name = random_card.get("name")
                    relation = None  # No relation for a randomly selected card
                    priority = 0  # Default priority
                else:
                    # All cards have been processed, pick any random card
                    total_cards = cards_collection.count_documents({})
                    random_skip = random.randint(0, total_cards - 1)
                    random_card = cards_collection.find().skip(random_skip).limit(1).next()
                    next_card_name = random_card.get("name")
                    processed_cards.discard(next_card_name)  # Allow reprocessing
                    relation = None
                    priority = 0

                print(f"Randomly selected card: {next_card_name}")
            else:
                # Sort the queue by priority (highest first)
                card_queue.sort(key=lambda x: x[2], reverse=True)

                # Get the highest priority card
                next_card_name, relation, priority = card_queue.pop(0)

            print(f"\n{'=' * 50}")
            print(f"PROCESSING {'QUEUED' if relation else 'RANDOM'} CARD #{cards_analyzed + 1}")
            print(f"Card: {next_card_name}" + (f" (Priority: {priority})" if relation else ""))
            print(f"Queue size: {len(card_queue)} cards remaining")
            print(f"{'=' * 50}")

            # Check if card exists in database
            card_doc = cards_collection.find_one({"name": next_card_name})
            if not card_doc:
                print(f"Card '{next_card_name}' not found in database. Skipping.")
                continue

            # Analyze the card
            result = analyze_single_card(temperature, topic, model, cards_collection,
                                         card_doc, relation)

            if not result:
                print(f"Failed to analyze '{next_card_name}'. Continuing with next card.")
                continue

            # Track the new card's details
            current_card_name, current_card_id, current_analysis_id, new_word_counts = result
            processed_cards.add(current_card_name)
            previous_card_id = current_card_id
            previous_analysis_id = current_analysis_id
            cards_analyzed += 1

            # Add newly mentioned cards to the queue
            new_additions = 0
            for card_name, count in new_word_counts.most_common():
                if card_name not in processed_cards and not any(card_name == q[0] for q in card_queue):
                    relation = {
                        "card_id": current_card_id,
                        "analysis_id": current_analysis_id,
                        "relationship_type": "mentioned_in",
                        "mention_count": count
                    }
                    card_queue.append((card_name, relation, count))
                    new_additions += 1

            # Provide queue stats
            if new_additions > 0:
                print(f"\nAdded {new_additions} new cards to the queue")

            print(f"\nQueue status: {len(card_queue)} cards")
            print(f"Cards processed so far: {cards_analyzed}")

            # Periodically show the top items in the queue (every 5 cards or when requested)
            if cards_analyzed % 5 == 0 or new_additions > 0:
                print("\nTop cards in queue:")
                if card_queue:
                    for i, (card_name, _, priority) in enumerate(card_queue[:5]):
                        print(f"{i + 1}. {card_name} (priority: {priority})")
                    if len(card_queue) > 5:
                        print(f"...and {len(card_queue) - 5} more")
                else:
                    print("Queue is currently empty - will select random card next")

            # Optional: Save progress periodically
            if cards_analyzed % 10 == 0:
                print(f"\nProgress checkpoint: {cards_analyzed} cards analyzed")

    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Saving progress...")
        # Here you could implement saving the current queue state if needed
    except Exception as e:
        print(f"\n\nError encountered: {str(e)}")
        # Log the error for debugging

    print(f"\nAnalysis complete. Processed {cards_analyzed} cards total.")
    print(f"Sample of processed cards: {', '.join(list(processed_cards)[:10])}")

    if card_queue:
        print(f"\nRemaining in queue: {len(card_queue)} cards")
        print(f"You can run this function again to continue processing.")

    return True


def analyze_cards_in_order(temperature=0.919, topic="commander_deck", model="gemma3:1b", max_cards=None):
    """
    Analyze cards sequentially in the order they appear in the database.

    Parameters:
    - temperature: Temperature setting for text generation
    - topic: Analysis topic (e.g., "commander_deck")
    - model: LLM model to use
    - max_cards: Maximum number of cards to analyze (None for unlimited)

    Returns:
    - bool: Success status
    """
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["mtgdbmongo"]
    cards_collection = db["cards"]

    # Get all cards that haven't been analyzed yet
    unanalyzed_query = {"card_analysis_date": {"$exists": False}}
    all_cards_cursor = cards_collection.find(unanalyzed_query)

    # Count how many cards will be processed
    total_unanalyzed = cards_collection.count_documents(unanalyzed_query)

    # Process each card in sequence
    cards_analyzed = 0

    print(f"\n{'=' * 50}")
    print(f"STARTING SEQUENTIAL CARD ANALYSIS")
    print(f"{'=' * 50}")

    try:
        for card_doc in all_cards_cursor:
            # Extract card details
            card_name = card_doc.get("name", "Unknown Card")
            card_id = card_doc.get("_id")

            print(f"\n{'=' * 50}")
            print(f"PROCESSING CARD #{cards_analyzed + 1}" +
                  (f" OF {max_cards}" if max_cards else f" OF {total_unanalyzed}"))
            print(f"Card: {card_name}")
            print(f"{'=' * 50}")

            # Check if this card has been analyzed before
            previously_analyzed = "card_analysis_date" in card_doc
            if previously_analyzed:
                last_analysis = card_doc.get("card_analysis_date")
                print(f"Note: This card was previously analyzed on {last_analysis}")

            # Generate the analysis
            print(f"\nGenerating analysis for {card_name}...")

            prompt = f"""
            Analyze the Magic: The Gathering card '{card_name}' for {topic}.

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
            analysis_id = store_analysis_in_db(card_name, card_id, topic, analysis, temperature)

            # Update the card document with analysis timestamp
            current_time = datetime.datetime.now()
            cards_collection.update_one(
                {"_id": card_id},
                {"$set": {
                    "card_analysis_date": current_time,
                    "last_analysis_id": analysis_id
                }}
            )

            cards_analyzed += 1

            # Optional: Save progress periodically
            if cards_analyzed % 10 == 0:
                print(f"\nProgress checkpoint: {cards_analyzed} cards analyzed")

    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Saving progress...")
    except Exception as e:
        print(f"\n\nError encountered: {str(e)}")
        # Log the error for debugging

    print(f"\nAnalysis complete. Processed {cards_analyzed} cards total.")
    return True

def find_cards_needing_analysis():
    """
    Identifies cards that need analysis by comparing the cards collection
    with the card_analyses collection.

    Returns:
    - list: List of card IDs that need analysis
    """
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["mtgdbmongo"]
    cards_collection = db["cards"]
    analyses_collection = db["card_analyses"]

    # Get all card IDs
    all_card_ids = [card["_id"] for card in cards_collection.find({}, {"_id": 1})]

    # Get all analyzed card IDs
    analyzed_card_ids = [analysis["card_id"] for analysis in analyses_collection.find({}, {"card_id": 1})]

    # Find cards that don't have analysis yet
    cards_needing_analysis = [card_id for card_id in all_card_ids if card_id not in analyzed_card_ids]

    print(f"Total cards: {len(all_card_ids)}")
    print(f"Analyzed cards: {len(analyzed_card_ids)}")
    print(f"Cards needing analysis: {len(cards_needing_analysis)}")

    return cards_needing_analysis

def process_cards_queue(temperature=0.919, topic="commander_deck", model="gemma3:1b", max_cards=None):
    """
    Process cards that need analysis by creating a queue and analyzing them sequentially.

    Parameters:
    - temperature: Temperature setting for text generation
    - topic: Analysis topic (e.g., "commander_deck")
    - model: LLM model to use
    - max_cards: Maximum number of cards to analyze (None for unlimited)

    Returns:
    - bool: Success status
    """
    # Get card IDs that need analysis
    cards_needing_analysis = find_cards_needing_analysis()

    # Limit the number of cards if max_cards is specified
    if max_cards and max_cards < len(cards_needing_analysis):
        cards_to_process = cards_needing_analysis[:max_cards]
        print(f"Processing {max_cards} cards out of {len(cards_needing_analysis)} cards needing analysis")
    else:
        cards_to_process = cards_needing_analysis
        print(f"Processing all {len(cards_needing_analysis)} cards needing analysis")

    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["mtgdbmongo"]
    cards_collection = db["cards"]

    # Process each card in sequence
    cards_analyzed = 0

    print(f"\n{'=' * 50}")
    print(f"STARTING CARD ANALYSIS QUEUE")
    print(f"{'=' * 50}")

    try:
        for card_id in cards_to_process:
            # Get the card document
            card_doc = cards_collection.find_one({"_id": card_id})

            if not card_doc:
                print(f"Card with ID {card_id} not found. Skipping.")
                continue

            # Extract card details
            card_name = card_doc.get("name", "Unknown Card")

            print(f"\n{'=' * 50}")
            print(f"PROCESSING CARD #{cards_analyzed + 1} OF {len(cards_to_process)}")
            print(f"Card: {card_name}")
            print(f"{'=' * 50}")

            # Generate the analysis
            print(f"\nGenerating analysis for {card_name}...")

            prompt = f"""
            Analyze the Magic: The Gathering card '{card_name}' for {topic}.

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
            analysis_id = store_analysis_in_db(card_name, card_id, topic, analysis, temperature)

            # Update the card document with analysis timestamp
            current_time = datetime.datetime.now()
            cards_collection.update_one(
                {"_id": card_id},
                {"$set": {
                    "card_analysis_date": current_time,
                    "last_analysis_id": analysis_id
                }}
            )

            cards_analyzed += 1

            # Optional: Save progress periodically
            if cards_analyzed % 10 == 0:
                print(f"\nProgress checkpoint: {cards_analyzed} cards analyzed")

    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Saving progress...")
    except Exception as e:
        print(f"\n\nError encountered: {str(e)}")
        # Log the error for debugging

    print(f"\nAnalysis complete. Processed {cards_analyzed} cards total.")
    return True

@app.route('/random-card-view')
def random_card_view():
    try:
        # Initialize MongoDB connection
        client = MongoClient('mongodb://localhost:27017/')
        db = client['mtgdbmongo']
        cards_collection = db['cards']

        # Use MongoDB's $sample aggregation to get a random document
        random_card = list(cards_collection.aggregate([
            {"$sample": {"size": 1}}
        ]))

        if not random_card:
            return "No cards found in the database", 404

        card = random_card[0]

        # MTG color mapping for styling
        color_map = {
            'W': {'name': 'White', 'color': '#F8E7B9', 'text': '#211D15'},
            'U': {'name': 'Blue', 'color': '#98C1D9', 'text': '#0D1A26'},
            'B': {'name': 'Black', 'color': '#3A3238', 'text': '#FFF'},
            'R': {'name': 'Red', 'color': '#E76F51', 'text': '#FFF'},
            'G': {'name': 'Green', 'color': '#54A24B', 'text': '#FFF'},
            'C': {'name': 'Colorless', 'color': '#BCBCBC', 'text': '#000'},
            'M': {'name': 'Multicolor', 'color': 'linear-gradient(135deg, #F8E7B9, #98C1D9, #3A3238, #E76F51, #54A24B)',
                  'text': '#000'}
        }

        # Determine card color for styling
        card_colors = card.get('colors', [])
        if not card_colors:
            card_color = color_map['C']  # Colorless default
        elif len(card_colors) > 1:
            card_color = color_map['M']  # Multicolor
        else:
            card_color = color_map.get(card_colors[0], color_map['C'])

        # Create HTML for the card
        html = ['<!DOCTYPE html>',
                '<html lang="en">',
                '<head>',
                '<title>MTG Card: {}</title>'.format(card.get('name', 'Random Card')),
                '<meta charset="UTF-8">',
                '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
                '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.1.1/css/all.min.css">',
                '<style>',
                '  :root {',
                f'    --card-color: {card_color["color"]};',
                f'    --text-color: {card_color["text"]};',
                '  }',
                '  * { box-sizing: border-box; }',
                '  body { font-family: "Segoe UI", Roboto, Arial, sans-serif; margin: 0; padding: 0; background: #f0f0f0; color: #333; line-height: 1.6; }',
                '  .container { max-width: 1000px; margin: 0 auto; padding: 20px; }',
                '  .card { border-radius: 15px; overflow: hidden; background: white; box-shadow: 0 10px 30px rgba(0,0,0,0.15); margin-bottom: 40px; }',
                '  .card-header { background: var(--card-color); color: var(--text-color); padding: 20px 30px; position: relative; }',
                '  .card-name { margin: 0; font-size: 2.2em; text-shadow: 0 1px 2px rgba(0,0,0,0.1); }',
                '  .card-type { margin: 5px 0 0; font-size: 1.2em; opacity: 0.9; }',
                '  .card-mana { position: absolute; top: 20px; right: 30px; font-size: 1.5em; }',
                '  .card-body { padding: 30px; display: flex; flex-wrap: wrap; gap: 30px; }',
                '  .card-images { flex: 1; min-width: 300px; }',
                '  .card-details { flex: 1; min-width: 300px; }',
                '  .card-gallery { display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 20px; }',
                '  .card-image { flex: 0 0 auto; max-width: 100%; position: relative; }',
                '  .card-image img { max-width: 100%; border-radius: 10px; box-shadow: 0 5px 15px rgba(0,0,0,0.2); transition: transform 0.3s ease; }',
                '  .card-image img:hover { transform: scale(1.03); }',
                '  .image-caption { position: absolute; bottom: 10px; left: 10px; background: rgba(0,0,0,0.7); color: white; ',
                '                   padding: 5px 10px; border-radius: 5px; font-size: 0.8em; }',
                '  .fullscreen-overlay { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; ',
                '                       background: rgba(0,0,0,0.9); z-index: 1000; justify-content: center; align-items: center; }',
                '  .fullscreen-image { max-width: 90%; max-height: 90%; }',
                '  .close-fullscreen { position: absolute; top: 20px; right: 30px; color: white; font-size: 2em; cursor: pointer; }',
                '  .section { margin-bottom: 25px; }',
                '  .section-title { margin: 0 0 15px; font-size: 1.4em; color: #444; border-bottom: 2px solid var(--card-color); padding-bottom: 8px; }',
                '  .oracle-text { background: #f8f8f8; padding: 20px; border-radius: 10px; white-space: pre-line; margin-bottom: 25px; }',
                '  .card-prop { margin: 12px 0; display: flex; }',
                '  .prop-name { font-weight: 600; width: 140px; color: #555; }',
                '  .prop-value { flex: 1; }',
                '  .flavor-text { font-style: italic; color: #666; background: #f8f8f8; padding: 15px; border-radius: 10px; margin: 15px 0; border-left: 4px solid var(--card-color); }',
                '  .legalities { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; }',
                '  .legality { padding: 5px 10px; border-radius: 5px; font-size: 0.9em; }',
                '  .legal { background: #d4edda; color: #155724; }',
                '  .not-legal { background: #f8d7da; color: #721c24; }',
                '  .restricted { background: #fff3cd; color: #856404; }',
                '  .banned { background: #862e2c; color: white; }',
                '  .price-info { background: #e9ecef; padding: 15px; border-radius: 10px; margin-top: 20px; }',
                '  .set-symbol { display: inline-block; margin-left: 10px; width: 20px; height: 20px; }',
                '  .set-info { display: flex; align-items: center; }',
                '  .card-footer { padding: 20px 30px; background: #f8f8f8; border-top: 1px solid #eee; text-align: center; }',
                '  .controls { display: flex; justify-content: center; gap: 15px; margin-bottom: 20px; }',
                '  .control-button { display: inline-block; background: var(--card-color); color: var(--text-color); padding: 10px 20px; ',
                '                   border-radius: 5px; text-decoration: none; transition: opacity 0.2s ease; border: none; font-size: 1em; cursor: pointer; }',
                '  .control-button:hover { opacity: 0.9; }',
                '  .json-data { display: none; background: #f5f5f5; padding: 15px; border-radius: 10px; overflow: auto; max-height: 400px; margin-top: 20px; }',
                '  .visible { display: block; }',
                '  .colors-container { display: flex; gap: 5px; }',
                '  .color-dot { width: 20px; height: 20px; border-radius: 50%; display: inline-block; }',
                '  .tooltip { position: relative; display: inline-block; }',
                '  .tooltip .tooltiptext { visibility: hidden; width: 120px; background-color: #555; color: #fff; text-align: center; ',
                '                         border-radius: 6px; padding: 5px 0; position: absolute; z-index: 1; bottom: 125%; left: 50%; margin-left: -60px; ',
                '                         opacity: 0; transition: opacity 0.3s; }',
                '  .tooltip:hover .tooltiptext { visibility: visible; opacity: 1; }',
                '  .loading { display: none; text-align: center; padding: 20px; }',
                '  .spinner { border: 4px solid rgba(0, 0, 0, 0.1); width: 36px; height: 36px; border-radius: 50%; border-left-color: var(--card-color); ',
                '            animation: spin 1s linear infinite; display: inline-block; }',
                '  @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }',
                '  @media (max-width: 768px) {',
                '    .card-header { padding: 15px; }',
                '    .card-name { font-size: 1.8em; }',
                '    .card-mana { position: static; margin-top: 10px; text-align: left; }',
                '    .card-body { padding: 15px; }',
                '  }',
                '</style>',
                '</head>',
                '<body>',
                '<div class="container">',
                '<div class="card">']

        # Card header
        html.append('<div class="card-header">')
        html.append(f'<h1 class="card-name">{card.get("name", "Unknown Card")}</h1>')

        # Type line
        if 'type_line' in card:
            html.append(f'<div class="card-type">{card["type_line"]}</div>')

        # Mana cost
        if 'mana_cost' in card:
            html.append(f'<div class="card-mana">{card["mana_cost"]}</div>')

        html.append('</div>')  # Close card-header

        # Card body
        html.append('<div class="card-body">')

        # Images section
        html.append('<div class="card-images">')
        html.append('<div class="section">')
        html.append('<h2 class="section-title">Card Images</h2>')
        html.append('<div class="card-gallery">')

        # Specifically check for and display image_uris.normal and image_uris.art_crop
        images_added = False

        # Helper function to add an image to the gallery
        def add_image_to_gallery(image_url, image_name):
            nonlocal images_added
            image_id = image_name.lower().replace(' ', '-')

            html.append(f'<div class="card-image" onclick="showFullscreen(\'{image_id}\')">')
            html.append(f'<img id="{image_id}" src="{image_url}" alt="{image_name}" />')
            html.append(f'<div class="image-caption">{image_name}</div>')
            html.append('</div>')
            images_added = True

        # Check for nested image_uris structure (standard Scryfall format)
        if 'image_uris' in card and isinstance(card['image_uris'], dict):
            # Check for normal image URL
            if 'normal' in card['image_uris'] and card['image_uris']['normal']:
                add_image_to_gallery(card['image_uris']['normal'], 'Normal View')

            # Check for art crop image URL
            if 'art_crop' in card['image_uris'] and card['image_uris']['art_crop']:
                add_image_to_gallery(card['image_uris']['art_crop'], 'Art Crop')

        # Check for double-faced cards or cards with multiple faces
        elif 'card_faces' in card and isinstance(card['card_faces'], list):
            for i, face in enumerate(card['card_faces']):
                if 'image_uris' in face and isinstance(face['image_uris'], dict):
                    face_name = face.get('name', f'Face {i + 1}')

                    # Check for normal image URL
                    if 'normal' in face['image_uris'] and face['image_uris']['normal']:
                        add_image_to_gallery(face['image_uris']['normal'], f'{face_name} - Normal View')

                    # Check for art crop image URL
                    if 'art_crop' in face['image_uris'] and face['image_uris']['art_crop']:
                        add_image_to_gallery(face['image_uris']['art_crop'], f'{face_name} - Art Crop')

        if not images_added:
            html.append(
                '<p>No images available for this card. The expected image_uris.normal and image_uris.art_crop fields were not found.</p>')

        html.append('</div>')  # Close card-gallery
        html.append('</div>')  # Close section
        html.append('</div>')  # Close card-images

        # Card details section
        html.append('<div class="card-details">')

        # Oracle text
        if 'oracle_text' in card and card['oracle_text']:
            html.append('<div class="section">')
            html.append('<h2 class="section-title">Oracle Text</h2>')
            html.append(f'<div class="oracle-text">{card["oracle_text"]}</div>')
            html.append('</div>')

        # Flavor text
        if 'flavor_text' in card and card['flavor_text']:
            html.append(f'<div class="flavor-text">{card["flavor_text"]}</div>')

        # Card properties
        html.append('<div class="section">')
        html.append('<h2 class="section-title">Card Properties</h2>')

        # Show P/T for creatures
        if 'power' in card and 'toughness' in card:
            html.append('<div class="card-prop">')
            html.append('<div class="prop-name">Power/Toughness</div>')
            html.append(f'<div class="prop-value">{card["power"]}/{card["toughness"]}</div>')
            html.append('</div>')

        # Colors
        if 'colors' in card and card['colors']:
            html.append('<div class="card-prop">')
            html.append('<div class="prop-name">Colors</div>')
            html.append('<div class="prop-value colors-container">')

            for color in card['colors']:
                color_info = color_map.get(color, color_map['C'])
                html.append(f'<div class="tooltip">')
                html.append(
                    f'<div class="color-dot" style="background-color: {color_info["color"] if "linear-gradient" not in color_info["color"] else "#ccc"};"></div>')
                html.append(f'<span class="tooltiptext">{color_info["name"]}</span>')
                html.append('</div>')

            html.append('</div>')  # Close prop-value
            html.append('</div>')  # Close card-prop

        # Rarity
        if 'rarity' in card:
            html.append('<div class="card-prop">')
            html.append('<div class="prop-name">Rarity</div>')
            html.append(f'<div class="prop-value">{card["rarity"].title()}</div>')
            html.append('</div>')

        # Set info
        if 'set_name' in card:
            html.append('<div class="card-prop">')
            html.append('<div class="prop-name">Set</div>')
            html.append(f'<div class="prop-value set-info">{card["set_name"]}')

            if 'set' in card:
                html.append(f' ({card["set"].upper()})')

            # Could add set symbol here if available

            html.append('</div>')  # Close prop-value
            html.append('</div>')  # Close card-prop

        # Artist
        if 'artist' in card:
            html.append('<div class="card-prop">')
            html.append('<div class="prop-name">Artist</div>')
            html.append(f'<div class="prop-value">{card["artist"]}</div>')
            html.append('</div>')

        # Keywords
        if 'keywords' in card and card['keywords']:
            html.append('<div class="card-prop">')
            html.append('<div class="prop-name">Keywords</div>')
            html.append(f'<div class="prop-value">{", ".join(card["keywords"])}</div>')
            html.append('</div>')

        # Collector number
        if 'collector_number' in card:
            html.append('<div class="card-prop">')
            html.append('<div class="prop-name">Collector Number</div>')
            html.append(f'<div class="prop-value">{card["collector_number"]}</div>')
            html.append('</div>')

        html.append('</div>')  # Close section

        # Legalities section
        if 'legalities' in card and isinstance(card['legalities'], dict):
            html.append('<div class="section">')
            html.append('<h2 class="section-title">Format Legality</h2>')
            html.append('<div class="legalities">')

            for format_name, status in card['legalities'].items():
                status_class = 'legal' if status == 'legal' else 'not-legal' if status == 'not_legal' else status
                html.append(
                    f'<div class="legality {status_class}">{format_name}: {status.replace("_", " ").title()}</div>')

            html.append('</div>')  # Close legalities
            html.append('</div>')  # Close section

        # Price information if available
        if 'prices' in card and isinstance(card['prices'], dict):
            html.append('<div class="section">')
            html.append('<h2 class="section-title">Price Information</h2>')
            html.append('<div class="price-info">')

            for price_type, price in card['prices'].items():
                if price and price != "null":
                    currency = '$' if price_type.endswith('usd') else '€' if price_type.endswith('eur') else ''
                    html.append(
                        f'<div><strong>{price_type.replace("_", " ").title()}:</strong> {currency}{price}</div>')

            html.append('</div>')  # Close price-info
            html.append('</div>')  # Close section

        html.append('</div>')  # Close card-details
        html.append('</div>')  # Close card-body

        # Card footer with controls
        html.append('<div class="card-footer">')
        html.append('<div class="controls">')
        html.append(
            '<a href="/random-card-view" class="control-button"><i class="fas fa-random"></i> Get Another Card</a>')
        html.append(
            '<button onclick="toggleJson()" class="control-button"><i class="fas fa-code"></i> Toggle JSON Data</button>')
        html.append('</div>')

        # Convert card to a JSON-serializable format
        import json
        from bson import ObjectId

        class MongoJSONEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, ObjectId):
                    return str(obj)
                elif isinstance(obj, bytes):
                    return "[binary data]"
                return super().default(obj)

        card_json = json.dumps(card, indent=2, cls=MongoJSONEncoder)

        html.append(f'<pre id="jsonData" class="json-data">{card_json}</pre>')
        html.append('</div>')  # Close card-footer

        html.append('</div>')  # Close card

        # Loading indicator
        html.append('<div id="loading" class="loading">')
        html.append('<div class="spinner"></div>')
        html.append('<p>Loading next card...</p>')
        html.append('</div>')

        html.append('</div>')  # Close container

        # Fullscreen image overlay
        html.append('<div id="fullscreenOverlay" class="fullscreen-overlay" onclick="hideFullscreen()">')
        html.append('<div class="close-fullscreen">&times;</div>')
        html.append('<img id="fullscreenImage" class="fullscreen-image" src="" alt="Fullscreen image">')
        html.append('</div>')

        # JavaScript for interactivity
        html.append('<script>')
        html.append('function toggleJson() {')
        html.append('  var jsonElement = document.getElementById("jsonData");')
        html.append('  jsonElement.classList.toggle("visible");')
        html.append('}')

        html.append('function showFullscreen(imageId) {')
        html.append('  var original = document.getElementById(imageId);')
        html.append('  var fullscreen = document.getElementById("fullscreenImage");')
        html.append('  var overlay = document.getElementById("fullscreenOverlay");')
        html.append('  fullscreen.src = original.src;')
        html.append('  overlay.style.display = "flex";')
        html.append('  document.body.style.overflow = "hidden";')
        html.append('}')

        html.append('function hideFullscreen() {')
        html.append('  var overlay = document.getElementById("fullscreenOverlay");')
        html.append('  overlay.style.display = "none";')
        html.append('  document.body.style.overflow = "auto";')
        html.append('}')

        html.append('// Show loading indicator when getting a new card')
        html.append('document.addEventListener("DOMContentLoaded", function() {')
        html.append('  var randomButton = document.querySelector(\'a[href="/random-card-view"]\');')
        html.append('  randomButton.addEventListener("click", function(e) {')
        html.append('    document.getElementById("loading").style.display = "block";')
        html.append('  });')
        html.append('});')

        # Close fullscreen on escape key
        html.append('document.addEventListener("keydown", function(e) {')
        html.append('  if (e.key === "Escape") {')
        html.append('    hideFullscreen();')
        html.append('  }')
        html.append('});')

        html.append('</script>')

        html.append('</body></html>')

        return '\n'.join(html)

    except Exception as e:
        import traceback
        print(f"Error retrieving random card: {e}")
        print(traceback.format_exc())
        return f"Error: {str(e)}", 500

    finally:
        if 'client' in locals():
            client.close()

def main():
    # Parse command line arguments for options
    parser = argparse.ArgumentParser(description="MTG Card Analysis Tool")

    # Optional arguments with defaults
    parser.add_argument("--temperature", type=float, default=1.0,
                        help="Temperature setting for text generation (default: 1.0)")
    parser.add_argument("--topic", type=str, default="commander_deck",
                        help="Analysis topic (default: commander_deck)")
    parser.add_argument("--model", type=str, default="gemma3:1b",
                        help="Ollama model to use (default: gemma3:1b)")
    parser.add_argument("--max-cards", type=int, default=None,
                        help="Maximum number of cards to analyze (default: unlimited)")
    parser.add_argument("--mode", type=str, choices=["queue", "chain", "sequential"], default="sequential",
                        help="Analysis mode: queue (priority-based), chain (related cards), or sequential (in-order) (default: sequential)")
    parser.add_argument("--chain-depth", type=int, default=2,
                        help="Number of related cards to analyze in the chain (default: 2)")
    parser.add_argument("--continue-when-empty", action="store_true", default=True,
                        help="Continue with random cards when queue is empty (default: True)")

    args = parser.parse_args()

    process_cards_queue(
        temperature=0.919,
        topic="commander_format",
        model="gemma3:1b",
        max_cards=500000
    )

    return 0

if __name__ == "__main__":
    import datetime  # Added for timestamp in database

    main()



