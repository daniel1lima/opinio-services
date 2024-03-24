from flask import Flask, jsonify
import subprocess

app = Flask(__name__)

@app.route('/run-script')
#need to match above with axios call to /run-script
def run_script():
    # Run the Python script
    subprocess.run(["python", "path/to/grabber.py"])

    # Return the path to the generated JSON file
    # Adjust the path as per your script's output
    json_file_path = "path/to/generated.json"
    return jsonify({"json_file_path": json_file_path})

if __name__ == '__main__':
    # Run the Flask app on a custom host and port
    app.run(host='0.0.0.0', port=8080, debug = True)

    #change port as needed