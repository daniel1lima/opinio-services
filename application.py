import json
import os
import uuid
from flask import Flask, jsonify, request
import redis
from rq import Queue
from connectors.worker_tasks import start_fetch

from modules.fetch_reviews import fetch_and_analyze_yelp_reviews, fetch_reviews
from connectors.factory import ConnectorFactory
from models.models import UserModel, ConnectionModel, CompanyModel
from modules.logger_setup import setup_logger

from models.status_constants import status_constants
from flask_cors import CORS


app = Flask(__name__)
CORS(app)

redis_conn = redis.Redis()
q = Queue("default", connection=redis_conn)


@app.route("/")
def home():
    return "Welcome to the Flask App!"


@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "Opinio is working!"})


@app.route("/fetch_yelp_reviews", methods=["POST"])
def fetch_reviews_wrapper():
    request_data = request.get_json()
    business_id = request_data.get("business_id")
    n_reviews = request_data.get("n_reviews", 10)
    industry = request_data.get("industry", "")

    result = fetch_and_analyze_yelp_reviews(business_id, n_reviews, industry)
    status_code = result.get("status", 200)
    return jsonify(result), status_code


@app.route("/add_connection", methods=["POST"])
def add_connection():
    request_data = request.get_json()
    try:
        if request_data:
            logger = setup_logger("add_connection.log")
            logger.info(f"Adding connection:{json.dumps(request_data)}")
            # Create the desired object structure
            connector = {
                "type": request_data["name"],  # Use the name for type
                "config": {
                    field["label"]
                    .strip()
                    .lower()
                    .replace(" ", "_"): field["value"]
                    .strip()
                    .lower()
                    for field in request_data["fields"]
                },  # Convert to list
                "last_sync": request_data.get(
                    "last_sync", ""
                ),  # Add last_sync attribute
            }

            company_id = request_data.get("company_id")

            company = CompanyModel.get_company_by_id(company_id)

            result = company.add_connector(connector)

            return jsonify(result), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 400


@app.route("/remove_connection", methods=["POST"])
def remove_connection():
    logger = setup_logger("remove_connector.log")
    request_data = request.get_json()
    if request_data:
        logger = setup_logger("remove_connection.log")
        logger.info(f"Removing connection: {json.dumps(request_data)}")

        connector_type = request_data.get("type")
        if not connector_type:  # Check if 'type' is present
            return jsonify({"message": "'type' key is required"}), 400

        company_id = request_data.get("company_id")

        company = CompanyModel.get_company_by_id(company_id)
        result = company.remove_connector(
            connector_type
        )  # Call the remove_connector method

        return jsonify(result), 200


@app.route("/reviews", methods=["POST"])
def sync_reviews_wrapper():
    logger = setup_logger("reviews.log")
    request_data = request.get_json()
    user_connectors = request_data.get("connectors", None)  # Array of string
    company_id = request_data.get("company_id", None)

    if not company_id:
        return (
            jsonify(
                {
                    "status": status_constants.STATUS_FAILED,
                    "message": "User ID Required in request",
                }
            ),
            400,
        )

    connectors = []

    try:
        company = CompanyModel.get_company_by_id(company_id)
    except Exception as e:
        logger.error(f"Failed to fetch company by ID: {e}")
        return jsonify({"status": "error", "message": "Failed to fetch company"}), 500

    try:
        if user_connectors:
            logger.info(
                f"Fetching reviews with connector data:{json.dumps(user_connectors)}"
            )
            connectors = [c for c in company.connectors if c.type in user_connectors]
        else:
            connectors = company.connectors

        jobs = []

        job_id = uuid.uuid4()
        jobs.append(job_id)

        # q.enqueue(start_fetch(), job_id = job_id

        start_fetch(connectors)

        return jsonify({"status": "Jobs are in progress", "data": str(jobs)}), 202

    except Exception as e:
        logger.info("An error occurred while processing user data")
        return (
            jsonify({"status": status_constants.STATUS_FAILED, "message": str(e)}),
            400,
        )


if __name__ == "__main__":
    port = 5000
    print(f"Running on http://localhost:{port}")
    app.run(debug=True, host="0.0.0.0", port=port)
