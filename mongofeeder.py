import json
from pymongo import MongoClient
import csv

connection_string = "mongodb+srv://produhacks_user:produhacks23@produhacks24.7bfha2w.mongodb.net/?retryWrites=true&w=majority&appName=ProduHacks24"
client = MongoClient(connection_string)

db = client['Reviews']
username = "produhacks_user"
password = "produhacks23"

# Define the document to be inserted
# Read the CSV file

def push_document(path, name):
    with open(path, 'r') as file:
        reader = csv.DictReader(file)
        # Iterate over each row in the CSV file
        for row in reader:
            
            # Define the document to be inserted
            # document = {
            #     "gym_name": row['gym_name'],
            #     "overall_rating": row['overall_rating'],
            #     "review_text": row['review_text'],
            #     "date": row['date'],
            # }

            document = {
                "name": row['name'],
                "equipment": row['equipment'],
                "cleanliness": row['cleanliness'],
                "pricing": row['pricing'],
                "accessibility": row['accessibility'],
                "staff": row['staff'],
                "review_text": row['review_text'],
            }
            # Insert the document into the collection
            collection = db[name]
            collection.insert_one(document)


# push_document("DATA/final_results.csv", "yelp_reviews")
# push_document("DATA/google_reviews.csv", "google_reviews")
push_document("DATA/sentiment_reviews.csv", "sentiment_reviews")

