from flask import Flask, jsonify
import subprocess
import csv
from openai import OpenAI
import pandas as pd

from yelp_script import yelp_script
from gptscript import split_reviews
# from fitscorecalculator import calculate_all, calculate_fit_score
from mongofeeder import push_document

filepath = 'DATA/sentiment_reviews_withcount.csv'

app = Flask(__name__)

@app.route('/sentiment', methods=['GET'])
#need to match above with axios call to /run-script
def run_script():
    # # Run the Python scripts if we want
    # print("yelp script")
    # yelp_script("Gyms", 5)

    # print("splitting reviews")
    # split_reviews()

    # print("calculating all")
    # calculate_all()

    # print("pushing")
    # push_document("DATA/sentiment_reviews_withcount.csv", "sentiment_reviews")
    


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
    