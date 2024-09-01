import json
import os
import time
import uuid
from flask import Flask, jsonify, request, Response
import redis
from rq import Queue
from connectors.publish import publish_job_status
from connectors.worker_tasks import initial_onboarding, poll_new_reviews, resume_fetch

from modules.fetch_reviews import fetch_and_analyze_yelp_reviews, fetch_reviews
from connectors.factory import ConnectorFactory
from models.models import (
    InboxModel,
    ReviewModel,
    UserModel,
    ConnectionModel,
    CompanyModel,
    JobModel,
)
from modules.logger_setup import setup_logger

from models.status_constants import status_constants
from flask_cors import CORS

from dotenv import load_dotenv

load_dotenv(override=True)

import logging

# Add this near the top of your file, after the imports
logging.getLogger("werkzeug").setLevel(logging.ERROR)


app = Flask(__name__)
CORS(app)

redis_conn = redis.Redis()
q = Queue("default", connection=redis_conn)
pubsub = redis_conn.pubsub()


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
            logger = setup_logger(log_dir="logs/manage_connections")
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
    logger = setup_logger(log_dir="logs/manage_connections")
    request_data = request.get_json()

    if request_data:
        logger.info(f"Removing connection: {json.dumps(request_data)}")
        user_id = request_data.get("user_id")
        connector_type = request_data.get("type")
        if not connector_type:  # Check if 'type' is present
            return jsonify({"message": "'type' key is required"}), 400

        company_id = request_data.get("company_id")

        company = CompanyModel.get_company_by_id(company_id)
        result = company.remove_connector(
            connector_type, user_id
        )  # Call the remove_connector methodc

        return jsonify(result), 200


@app.route("/reviews", methods=["POST"])
def sync_reviews_wrapper():
    logger = setup_logger(log_dir="logs/ingest_reviews")
    request_data = request.get_json()
    user_connectors = request_data.get("connectors", None)
    user_id = request_data.get("user_id", None)
    company_id = request_data.get("company_id", None)
    action = request_data.get("action", "poll")  # New parameter to determine the action

    if not company_id:
        return (
            jsonify(
                {
                    "status": status_constants.STATUS_FAILED,
                    "message": "Company ID Required in request",
                }
            ),
            400,
        )

    try:
        company = CompanyModel.get_company_by_id(company_id)
        if company is None:
            return (
                jsonify(
                    {
                        "status": status_constants.STATUS_FAILED,
                        "message": f"Company with ID {company_id} does not exist",
                    }
                ),
                404,
            )
    except Exception as e:
        logger.error(f"Failed to fetch company by ID: {e}")
        return jsonify({"status": "error", "message": "Failed to fetch company"}), 500

    try:
        if user_connectors:
            logger.info(
                f"Fetching reviews with connector data: {json.dumps(user_connectors)}"
            )
            connectors = [c for c in company.connectors if c.type in user_connectors]
        else:
            connectors = company.connectors

        jobs = []

        for connector in connectors:
            job_id = str(uuid.uuid4())
            print(f"Job ID: {job_id}")
            if action == "initial":
                job = initial_onboarding(connector, company_id, user_id)
            elif action == "resume":
                job = resume_fetch(connector, company_id, user_id)
            else:  # Default to "poll"
                job = poll_new_reviews(connector, company_id, user_id)

            jobs.append(job_id)

        return jsonify({"status": "Jobs are in progress", "data": jobs}), 202

    except Exception as e:
        logger.error(
            f"An error occurred while processing user data: {str(e)}", exc_info=True
        )
        return (
            jsonify({"status": status_constants.STATUS_FAILED, "message": str(e)}),
            400,
        )


@app.route("/company_connections", methods=["GET"])
def get_company_connections():
    company_id = request.args.get("company_id")

    if not company_id:
        return jsonify({"status": "error", "message": "Company ID is required"}), 400

    try:
        company = CompanyModel.get_company_by_id(company_id)

        if company is None:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Company with ID {company_id} not found",
                    }
                ),
                404,
            )

        if company.connectors is None:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Company with ID {company_id} has no connections",
                    }
                ),
                404,
            )

        def serialize_config(config):
            return {
                k: v.serialize() if hasattr(v, "serialize") else v
                for k, v in config.attribute_values.items()
            }

        connections = [
            {
                "type": connector.type,
                "config": serialize_config(connector.config),
                "last_sync": connector.last_sync,
            }
            for connector in company.connectors
        ]

        return (
            jsonify(
                {
                    "status": "success",
                    "company_id": company_id,
                    "connections": connections,
                }
            ),
            200,
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/job_status_webhook", methods=["GET"])
def job_status_webhook():
    company_id = request.args.get("company_id")

    if not company_id:
        return jsonify({"status": "error", "message": "Company ID is required"}), 400

    def event_stream():
        channel = f"job_status:{company_id}"
        pubsub.subscribe(channel)

        try:
            # Send the initial job status or a "no job" message
            most_recent_job = JobModel.get_most_recent_job(company_id)
            if most_recent_job:
                job_data = {
                    "job_id": most_recent_job.job_id,
                    "company_id": most_recent_job.company_id,
                    "connector_type": most_recent_job.connector_type,
                    "status": most_recent_job.status,
                    "total_reviews_fetched": most_recent_job.total_reviews_fetched,
                    "last_sync": most_recent_job.last_sync,
                    "error_message": most_recent_job.error_message,
                    "created_at": most_recent_job.created_at,
                    "updated_at": most_recent_job.updated_at,
                }
            else:
                job_data = {"status": "no_job", "message": "No recent jobs found"}

            yield f"data: {json.dumps(job_data)}\n\n"

            while True:
                message = pubsub.get_message(timeout=5.0)  # Increased timeout
                if message:
                    if message["type"] == "message":
                        yield f"data: {message['data'].decode('utf-8')}\n\n"
                    elif message["type"] == "unsubscribe":
                        most_recent_job = JobModel.get_most_recent_job(company_id)
                        pubsub.subscribe(channel)
                        if most_recent_job:
                            job_data = {
                                "job_id": most_recent_job.job_id,
                                "company_id": most_recent_job.company_id,
                                "connector_type": most_recent_job.connector_type,
                                "status": most_recent_job.status,
                                "total_reviews_fetched": most_recent_job.total_reviews_fetched,
                                "last_sync": most_recent_job.last_sync,
                                "error_message": most_recent_job.error_message,
                                "created_at": most_recent_job.created_at,
                                "updated_at": most_recent_job.updated_at,
                            }
                            yield f"data: {json.dumps(job_data)}\n\n"

                else:
                    # Send a heartbeat every 30 seconds
                    if time.time() % 30 < 1:
                        yield f"data: {json.dumps({'status': 'heartbeat'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            pubsub.unsubscribe(channel)

    return Response(event_stream(), content_type="text/event-stream")


@app.route("/most_recent_job", methods=["GET"])
def get_most_recent_job():
    company_id = request.args.get("company_id")

    if not company_id:
        return jsonify({"status": "error", "message": "Company ID is required"}), 400

    try:
        job = JobModel.get_most_recent_job(company_id)

        if job is None:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"No jobs found for company with ID {company_id}",
                    }
                ),
                404,
            )

        job_data = {
            "job_id": job.job_id,
            "company_id": job.company_id,
            "connector_type": job.connector_type,
            "status": job.status,
            "total_reviews_fetched": job.total_reviews_fetched,
            "last_sync": job.last_sync,
            "error_message": job.error_message,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
        }

        return jsonify({"status": "success", "job": job_data}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/get_inbox_reviews", methods=["GET"])
def get_inbox_reviews():
    company_id = request.args.get("company_id")
    user_id = request.args.get("user_id")
    page = int(request.args.get("page", 1))  # Get the page number, default to 1
    per_page = int(
        request.args.get("page_size", 10)
    )  # Get the number of items per page, default to 10

    reviews = list(InboxModel.fetch_inbox_items_by_user_id(user_id))

    if not len(reviews):
        # Fetch reviews belonging to the company if user's inbox is empty
        company_reviews = list(
            ReviewModel.fetch_reviews_by_company_id(company_id)
        )  # Fetch company reviews
        if company_reviews:
            for review in company_reviews:
                InboxModel.create_inbox_item(
                    user_id, review
                )  # Create inbox review for each company review
            reviews = list(
                InboxModel.fetch_inbox_items_by_user_id(user_id)
            )  # Re-fetch inbox reviews after creation
        else:
            return (
                jsonify(
                    {"status": "error", "message": "No reviews found for the company"}
                ),
                404,
            )

    # Implement pagination
    start = (page - 1) * per_page
    end = start + per_page
    paginated_reviews = reviews[start:end]  # Slice the reviews list for pagination

    formatted_reviews = [review.to_simple_dict() for review in paginated_reviews]

    response = {
        "status": "success",
        "reviews": formatted_reviews,
        "total_reviews": len(reviews),
        "page": page,
        "per_page": per_page,
    }

    return jsonify(response), 200


@app.route("/update_inbox_item", methods=["POST"])
def update_inbox_item():
    request_data = request.get_json()
    user_id = request_data.get("user_id")
    review_id = request_data.get("review_id")
    is_starred = request_data.get("is_starred")
    is_read = request_data.get("is_read")
    labels = request_data.get("labels")

    if not user_id or not review_id:
        return (
            jsonify(
                {"status": "error", "message": "user_id and review_id are required"}
            ),
            400,
        )

    try:
        inbox_item = InboxModel.fetch_inbox_item_by_user_id_and_review_id(
            user_id, review_id
        )
        if not inbox_item:
            return jsonify({"status": "error", "message": "Inbox item not found"}), 404

        # Update fields if provided
        if is_starred is not None:
            inbox_item.is_starred = is_starred
        if is_read is not None:
            inbox_item.is_read = is_read
        if labels is not None:
            inbox_item.labels = labels

        inbox_item.save()  # Assuming there's a save method to persist changes

        return (
            jsonify(
                {"status": "success", "message": "Inbox item updated successfully"}
            ),
            200,
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/inbox_breakdown", methods=["GET"])
def inbox_breakdown():
    user_id = request.args.get("user_id")

    if not user_id:
        return jsonify({"status": "error", "message": "User ID is required"}), 400

    try:
        inbox_items = list(InboxModel.fetch_inbox_items_by_user_id(user_id))

        total_reviews = len(inbox_items)
        starred_reviews = sum(1 for item in inbox_items if item.is_starred)
        unread_reviews = sum(1 for item in inbox_items if not item.is_read)

        breakdown = {
            "total_reviews": total_reviews,
            "starred_reviews": starred_reviews,
            "unread_reviews": unread_reviews,
        }

        return jsonify({"status": "success", "data": breakdown}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = 5000
    print(f"Running on http://localhost:{port}")
    app.run(debug=True, host="0.0.0.0", port=port, threaded=True)
