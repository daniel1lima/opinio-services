from flask import Flask, jsonify
import subprocess
import csv



app = Flask(__name__)

@app.route('/run-script', methods=['GET'])
#need to match above with axios call to /run-script
def run_script():
    # Run the Python script
    # subprocess.run(["python", "path/to/grabber.py"])
    
    csv_file_path = "DATA/sentiment_reviews.csv"

    # Read the CSV file and convert it to a list of dictionaries
    data = []
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(dict(row))

    return jsonify(data)

    