from flask import Flask, jsonify, request
import redis
from redis import Queue
from Modules.fetch_reviews import fetch_reviews

app = Flask(__name__)

redis_conn = redis.Redis()
q = Queue("default", connection=redis_conn)


@app.route("/")
def home():
    return "Welcome to the Flask App!"


@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "Opinio is working!"})


@app.route("/fetch_reviews", methods=["POST"])
def fetch_reviews_wrapper():
    request_data = request.get_json()
    query = request_data["query"]
    page_count = request_data["page_count"]
    business_count = request_data["business_count"]

    print(request_data)

    q.enqueue(fetch_reviews, request_data)

    fetch_reviews(request_data)

    return jsonify({"status": "Opinio is working!"}, 200)


if __name__ == "__main__":
    app.run(debug=True)
