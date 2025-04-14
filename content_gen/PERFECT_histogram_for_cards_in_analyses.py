import re

import pymongo
import collections

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/infinitplex_dev")

# Select the desired collection
db = client["infinityplex_dev"]
collection = db["card_analyses"]

# Initialize an empty dictionary to store word frequencies
word_counts = {}

# Iterate over each document in the collection
for document in collection.find():
    # Extract the 'content' field from the document
    content = document["content"]

    # Split the content into words enclosed in double square brackets
    bracketed_terms = re.findall(r'\]\[(.*?)\]', content)

    # Iterate over each term in the bracketed terms
    for term in bracketed_terms:
        # Remove any extra brackets from the term
        term = term.strip('[]')

        if term in word_counts:
            # Increment the count if it's already in the dictionary
            word_counts[term] += 1
        else:
            # Initialize the count to 1 if it's not in the dictionary
            word_counts[term] = 1

# Print the histogram
print("Histogram of words enclosed in double square brackets:")
for word, count in sorted(word_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"[[{word}]]: {count}")