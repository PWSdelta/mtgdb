import pymongo
import collections
import re

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/infinitplex_dev")

# Select the desired collection
db = client["infinityplex_dev"]
collection = db["card_analyses"]

# Initialize an empty string to store the concatenated content
content_string = ""

# Iterate over each document in the collection
for document in collection.find():
    # Extract the 'content' field from the document
    content = document["content"]

    # Append the content to the string
    content_string += content + "\n"

# Use regular expression to find all terms enclosed in [[ ]]
pattern = r'\[\[(.*?)\]\]'
bracketed_terms = re.findall(pattern, content_string)

# Create a counter for these terms
word_counts = collections.Counter(bracketed_terms)

# Print the histogram
print("Histogram of words enclosed in double square brackets:")
for word, count in word_counts.most_common():
    print(f"[[{word}]]: {count}")