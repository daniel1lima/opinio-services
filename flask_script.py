from flask import Flask, jsonify
import subprocess
import csv

from yelp_script import yelp_script



app = Flask(__name__)

@app.route('/run-script', methods=['GET'])
#need to match above with axios call to /run-script
def run_script():
    # Run the Python script
    yelp_script("Gyms", 5)
    
    
    csv_file_path = "DATA/sentiment_reviews.csv"

    # Read the CSV file and convert it to a list of dictionaries
    data = []
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(dict(row))

    return jsonify(data)

    