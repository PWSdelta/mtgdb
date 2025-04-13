import argparse
import sys
import requests
import pymongo
import json
import datetime


def test_ollama_connection():
    """Test connection to Ollama API and check for available models"""
    print("\n=== Testing Ollama Connection ===")
    try:
        # Check if Ollama API is responsive
        response = requests.get("http://localhost:11434/api/tags", timeout=5)

        if response.status_code != 200:
            print(f"❌ Ollama API not responding correctly (status code: {response.status_code})")
            return False

        # Check available models
        models_data = response.json()
        if "models" not in models_data:
            print("❌ No models found in Ollama response")
            return False

        available_models = [model["name"] for model in models_data.get("models", [])]

        if not available_models:
            print("❌ No models available in Ollama")
            return False

        print(f"✅ Successfully connected to Ollama")
        print(f"✅ Available models: {', '.join(available_models)}")

        # Try to use gemma3:latest specifically
        if "gemma3:latest" in available_models:
            print(f"✅ gemma3:latest model is available")

            # Test a simple generation with gemma3:latest
            print("Testing simple generation with gemma3:latest...")
            gen_response = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": "gemma3:latest",
                    "prompt": "Generate a single sentence about Magic: The Gathering.",
                    "stream": False
                },
                timeout=30
            )

            if gen_response.status_code == 200:
                result = gen_response.json().get("response", "")
                print(f"✅ Successfully generated text with gemma3:latest")
                print(f"Sample output: {result[:100]}..." if len(result) > 100 else f"Sample output: {result}")
            else:
                print(f"❌ Failed to generate text with gemma3:latest (status code: {gen_response.status_code})")
        else:
            print(f"⚠️ gemma3:latest model is not available. Available models: {', '.join(available_models)}")

        return True

    except Exception as e:
        print(f"❌ Error connecting to Ollama: {str(e)}")
        return False


def test_mongodb_connection():
    """Test connection to MongoDB and create test collection"""
    print("\n=== Testing MongoDB Connection ===")
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)

        # Test the connection by accessing server info
        server_info = client.server_info()
        print(f"✅ Successfully connected to MongoDB version {server_info.get('version')}")

        # Create/access the database
        db = client["infinityplex_dev"]

        # Create/access the collection
        collection = db["card_analyses"]

        # Insert a test document
        test_doc = {
            "card_name": "Test Card",
            "analysis_type": "connection_test",
            "content": "This is a test document to verify MongoDB connection",
            "test_timestamp": str(datetime.datetime.now())
        }

        result = collection.insert_one(test_doc)

        if result.acknowledged:
            print(f"✅ Successfully inserted test document (id: {result.inserted_id})")

            # Retrieve the test document
            retrieved = collection.find_one({"card_name": "Test Card", "analysis_type": "connection_test"})
            if retrieved:
                print(f"✅ Successfully retrieved test document")

                # Clean up the test document
                delete_result = collection.delete_one({"_id": result.inserted_id})
                if delete_result.acknowledged:
                    print(f"✅ Successfully deleted test document")
                else:
                    print(f"⚠️ Failed to delete test document")
            else:
                print(f"❌ Failed to retrieve test document")
        else:
            print(f"❌ Failed to insert test document")

        return True

    except pymongo.errors.ServerSelectionTimeoutError:
        print("❌ Failed to connect to MongoDB: Server selection timed out")
        print("   Is the MongoDB server running on localhost:27017?")
        return False
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {str(e)}")
        return False


def generate_llm_response(prompt, model="gemma3:latest", temperature=1.0, stream=False):
    """Generate a response from the LLM using Ollama API"""
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "temperature": temperature,
                "stream": stream
            },
            timeout=451
        )

        if response.status_code == 200:
            return response.json().get("response", "")
        else:
            print(f"\n❌ Failed to generate response (status code: {response.status_code})")
            return None
    except Exception as e:
        print(f"\n❌ Error generating response: {str(e)}")
        return None


def store_analysis_in_db(card_name, analysis_type, content, temperature):
    """Store the analysis in MongoDB"""
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["infinityplex_dev"]
        collection = db["card_analyses"]

        # Remove the opening line
        opening_patterns = [
            f"Okay, let's analyze '{card_name}' for Commander decks. ",
            f"Okay, let's analyze '{card_name}' for {analysis_type}. ",
            f"Let's analyze the Magic: The Gathering card '{card_name}' for {analysis_type}. ",
            f"Analyzing '{card_name}' for {analysis_type}. "
        ]

        for pattern in opening_patterns:
            if content.startswith(pattern):
                content = content[len(pattern):].strip()

        # Remove the prompt at the end
        ending_patterns = [
            f"Would you like me to expand on any of these specific aspects for {card_name}?",
            "Would you like me to expand on any of these specific aspects for",
            "Would you like more information about another aspect",
            "Would you like to know more about any specific aspect"
        ]

        for pattern in ending_patterns:
            if pattern in content:
                content = content.split(pattern)[0].strip()

        # Format the primary card name with double brackets
        content = content.replace(card_name, f"[[{card_name}]]")

        # This would be a simplified approach - in practice, you'd need a more robust
        # method to identify card names, perhaps using a list or API lookup
        # For demonstration purposes, we're assuming some common cards might be mentioned
        common_cards = [
            "Sol Ring", "Counterspell", "Swords to Plowshares", "Birds of Paradise",
            "Lightning Bolt", "Rhystic Study", "Cultivate", "Wrath of God",
            "Cyclonic Rift", "Demonic Tutor", "Command Tower", "Arcane Signet",
            "Path to Exile", "Brainstorm", "Evolving Wilds", "Terramorphic Expanse"
            # Add more as needed
        ]

        for card in common_cards:
            # Replace the card name with bracketed version, but be careful not to
            # replace text that's already bracketed
            content = content.replace(card, f"[[{card}]]").replace("[[[[", "[[").replace("]]]]", "]]")

        doc = {
            "card_name": card_name,
            "analysis_type": analysis_type,
            "temperature": temperature,
            "content": content,
            "timestamp": str(datetime.datetime.now())
        }

        result = collection.insert_one(doc)
        if result.acknowledged:
            print(f"\n✅ Analysis saved to database (id: {result.inserted_id})")
            return True
        else:
            print(f"\n❌ Failed to save analysis to database")
            return False
    except Exception as e:
        print(f"\n❌ Error storing analysis: {str(e)}")
        return False


def analyze_card(card_name, temperature=1.0, topic="commander_deck"):
    """Analyze a given Magic: The Gathering card"""
    print(f"\n=== Analyzing {card_name} ===")
    print(f"Temperature: {temperature}")
    print(f"Topic: {topic}")

    # Test connections first
    # ollama_ok = test_ollama_connection()
    # mongo_ok = test_mongodb_connection()
    #
    # if not ollama_ok or not mongo_ok:
    #     print("\n⚠️ Connection issues detected. Please fix before continuing.")
    #     return False

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

    analysis = generate_llm_response(prompt, temperature=temperature)

    if not analysis:
        return False

    print("\n=== Analysis Results ===")
    print(analysis)

    # Store the initial analysis
    store_analysis_in_db(card_name, topic, analysis, temperature)

    print("Comprehensive analysis completed and saved to database.")
    return True


def analyze_random_card(card_name, temperature=1.0, topic="commander_deck"):
    """Analyze a given Magic: The Gathering card"""
    print(f"\n=== Analyzing {card_name} ===")
    print(f"Temperature: {temperature}")
    print(f"Topic: {topic}")

    # Test connections first
    # ollama_ok = test_ollama_connection()
    # mongo_ok = test_mongodb_connection()
    #
    # if not ollama_ok or not mongo_ok:
    #     print("\n⚠️ Connection issues detected. Please fix before continuing.")
    #     return False

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

    analysis = generate_llm_response(prompt, temperature=temperature)

    if not analysis:
        return False

    print("\n=== Analysis Results ===")
    print(analysis)

    # Store the initial analysis
    store_analysis_in_db(card_name, topic, analysis, temperature)

    print("Comprehensive analysis completed and saved to database.")
    return True


def parse_arguments():
    """Parse command line arguments with defaults"""
    parser = argparse.ArgumentParser(description="MTG Commander Card Analysis Tool")

    # Add card_name as a positional argument, but make it optional
    parser.add_argument("card_name", nargs="?", type=str, help="Name of the card to analyze")

    # Optional arguments with defaults
    parser.add_argument("--temperature", type=float, default=1.0,
                        help="Temperature setting for text generation (default: 1.0)")
    parser.add_argument("--topic", type=str, default="commander_deck",
                        help="Analysis topic (default: commander_deck)")

    # Parse the arguments
    return parser.parse_args()


def main():
    # Parse command line arguments for options
    args = parse_arguments()

    # If a card name was provided via command line, use it directly
    if args.card_name:
        analyze_card(args.card_name, args.temperature, args.topic)
        return 0

    # If no card name was provided, enter interactive mode
    while True:
        print("\nMTG Dual-Style Analysis Tool")
        print("1: Enter card name")
        print("0: Exit")

        choice = input("\nEnter your choice (1/0): ").strip()

        if choice == "0":
            print("Goodbye!")
            return 0
        elif choice == "1":
            card_name = input("\nWhich Card Name? ").strip()
            if card_name:
                analyze_card(card_name, args.temperature, args.topic)
            else:
                print("No card name entered. Please try again.")
        else:
            print("Invalid choice. Please enter 1 or 0.")


if __name__ == "__main__":
    sys.exit(main())