from flask import Flask, jsonify
import subprocess
import csv

from yelp_script import yelp_script
from gptscript import split_reviews



app = Flask(__name__)

@app.route('/run-script', methods=['GET'])
#need to match above with axios call to /run-script
def run_script():
    # Run the Python scripts if we want

    # yelp_script("Gyms", 5)
    # split_reviews()

    # compute scores
    
    
    csv_file_path = "DATA/sentiment_reviews.csv" #change this to the mongodb database

    # Read the CSV file and convert it to a list of dictionaries
    data = []
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(dict(row))

    return jsonify(data)

    