import json
import os
import uuid
from flask import Flask, jsonify, request
import redis
from rq import Queue

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
def fetch_reviews_total_wrapper():
    logger = setup_logger("reviews.log")
    request_data = request.get_json()
    connectors = request_data.get("connectors", None)  # Array of string
    user_id = request_data.get(
        "user_id", None
    )  # String adjacent to clerk ID in main DB

    if not user_id:
        return (
            jsonify(
                {
                    "status": status_constants.STATUS_FAILED,
                    "message": "User ID Required in request",
                }
            ),
            400,
        )

    try:
        if connectors:
            logger.info(
                f"Fetching reviews with connector data:{json.dumps(connectors)}"
            )
            # Create the desired object structure
            connectors = [
                {
                    "type": c,
                    "config": {
                        "business_id": next(
                            (
                                field["value"]
                                for field in connector["fields"]
                                if field["label"] == "Business ID"
                            ),
                            None,
                        )
                    },
                }
                for c in connectors
            ]
        else:
            user = Company.fetch_user(user_id)  # Corrected to use user_id
            connectors = user.connections

        jobs = []
        for c in connectors:
            fetched_connector = ConnectionModel.fetch_connector(
                user_id, c
            )  # Corrected to use ConnectionModel
            connector = ConnectorFactory(
                fetched_connector, c
            )  # Initialize connector with factory

            job_id = uuid.uuid4()
            jobs.append(job_id)  # Store job_id in jobs list

            q.enqueue(
                connector.fetch_new_reviews, job_id=job_id
            )  # Corrected to use q.enqueue

            logger.info(f"Enqueued {connector.__class__.__name__}")

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
