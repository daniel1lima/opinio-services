import os
from flask import Flask, jsonify, request
import redis

from Modules.fetch_reviews import fetch_and_analyze_reviews, fetch_reviews


app = Flask(__name__)

# redis_conn = redis.Redis()
# q = Queue("default", connection=redis_conn)


@app.route("/")
def home():
    return "Welcome to the Flask App!"


@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "Opinio is working!"})


@app.route("/fetch_reviews", methods=["POST"])
def fetch_reviews_wrapper():
    request_data = request.get_json()
    query = request_data.get("query", "")
    business_id = request_data.get("business_id")
    n_reviews = request_data.get("n_reviews", 10)
    industry = request_data.get("industry", "")

    result = fetch_and_analyze_reviews(query, business_id, n_reviews, industry)
    status_code = result.get("status", 200)
    return jsonify(result), status_code


if __name__ == "__main__":
    port = 5000
    print(f"Running on http://localhost:{port}")
    app.run(debug=True, host="0.0.0.0", port=port)
