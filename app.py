from flask import Flask, jsonify
import subprocess
import csv
from openai import OpenAI
import pandas as pd


from yelp_script import yelp_script
from gptscript import split_reviews
from fitscorecalculator import update_fit_scores_in_csv
from mongofeeder import push_document
from flask_cors import CORS

filepath = 'DATA/sentiment_reviews_withcount.csv'

app = Flask(__name__)
cors = CORS(app)

cors = CORS(app, resources={r"/*": {"origins": "*"}})
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/sentiment', methods=['GET'])
#need to match above with axios call to /run-script
def run_script():
    # Run the Python scripts if we want
    print("yelp script")
    yelp_script("Gyms", 5)

    # print("splitting reviews")
    # split_reviews()

    # print("calculating all")
    # update_fit_scores_in_csv("DATA/sentiment_reviews_withcount.csv")

    print("pushing")
    push_document("DATA/sentiment_reviews_withcount.csv", "reviews")
    


    # compute scores
    
    
    csv_file_path = "DATA/sentiment_reviews_withcount.csv" #change this to the mongodb database

    # Read the CSV file and convert it to a list of dictionaries
    data = []
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(dict(row))

    return jsonify(data)


@app.route('/insights', methods=['GET'])
def show_items():

    # compute scores
    
    
    csv_file_path = "DATA/action_items.csv" #change this to the mongodb database

    # Read the CSV file and convert it to a list of dictionaries
    data = []
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(dict(row))

    return jsonify(data)
    

# flask --app flask_script run