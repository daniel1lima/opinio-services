import datetime
import json
import time
import uuid
from flask import jsonify
import pandas as pd
import requests
import numpy as np
from Modules.logger_setup import setup_logger
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
from Modules.create_embeddings import analyze_reviews

logger = setup_logger()


class BrokenReviewsApi(Exception):
    def __init__(self, message="An error occurred with the reviews API"):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message


class Review(BaseModel):
    business_id: str
    business_name: str
    review_id: str = uuid.uuid4
    review_date: str = datetime.now()
    review_text: str
    review_url: str = Field(default="No Url")
    rating: int
    total_reviews: int
    platform_id: str


def fetch_reviews(business_id, n_reviews, retries=3):
    url = "https://red-flower-business-data.p.rapidapi.com/business-reviews"
    headers = {
        "x-rapidapi-key": "52f0c4fb7cmsh68305c1877afa13p1710b0jsn6ea7e5079542",
        "x-rapidapi-host": "red-flower-business-data.p.rapidapi.com",
    }

    reviews_list = []
    page_size = 10
    total_fetched = 0
    page = 1

    logger.info(
        f"Starting to fetch reviews for business_id: {business_id}, n_reviews: {n_reviews}"
    )

    while total_fetched < int(n_reviews):
        params = {
            "business_id": business_id,
            "page": page,
            "page_size": page_size,
            "num_pages": 1,
            "sort": "BEST_MATCH",
            "language": "en",
        }

        for attempt in range(retries):
            logger.info(
                f"Fetching page {page} (attempt {attempt + 1}) with params: {params}"
            )
            response = requests.get(url, headers=headers, params=params)
            logger.debug(f"Response status code: {response.status_code}")
            if response.status_code == 200:
                try:
                    reviews_data = response.json()
                    logger.debug(f"Response JSON: {reviews_data}")
                    data = reviews_data.get("data", {})
                    reviews = data.get("reviews", [])
                    total_reviews = data.get("total", 0)

                    for review in reviews:
                        review_entry = {
                            "business_id": business_id,
                            "business_name": business_id,
                            "review_id": review.get("review_id", "Unknown"),
                            "review_date": review.get("review_datetime_utc", None),
                            "review_text": review.get("review_text", ""),
                            "review_url": review.get("url", "No Url"),
                            "rating": review.get("review_rating", 0),
                            "total_reviews": total_reviews,
                            "platform_id": "001",
                        }
                        try:
                            validated_review = Review(**review_entry)
                            reviews_list.append(validated_review.dict())
                            logger.debug(f"Validated review: {validated_review.dict()}")
                        except ValidationError as e:
                            logger.error(f"Validation error: {e}")

                    total_fetched += len(reviews)
                    logger.info(
                        f"Fetched {len(reviews)} reviews, total fetched: {total_fetched}"
                    )

                    if len(reviews) < page_size:
                        logger.info("No more reviews to fetch, ending.")
                        break
                    page += 1
                    break
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error: {e}")
                    return []
                except KeyError as e:
                    logger.error(
                        f"KeyError: {e}. Response JSON structure might have changed."
                    )
                    return []
            elif response.status_code in [502, 503, 504]:
                logger.warning(
                    f"Server error {response.status_code}. Retrying... ({attempt + 1}/{retries})"
                )
                time.sleep(2**attempt)
            else:
                logger.error(
                    f"Error fetching reviews: {response.status_code} - {response.text}"
                )
                return []

    return reviews_list


INDUSTRY_MAP = {
    "restaurant": "001",
    "hotel": "002",
    "gym": "003",
    # Add more mappings as needed
}


def fetch_and_analyze_reviews(query, business_id, n_reviews, industry):
    logger.info(
        f"Fetching and analyzing reviews for business_id: {business_id}, industry: {industry}"
    )
    try:
        reviews_list = fetch_reviews(business_id, n_reviews)
    except Exception as e:
        logger.warning(f"No reviews fetched, returning empty list. {e}")
        # return {"status": 400, "message": str(e)}

    reviews = [review["review_text"] for review in reviews_list]
    logger.debug(f"Reviews texts for analysis: {reviews}")
    try:
        analyzed_reviews, _ = analyze_reviews(reviews)
        analyzed_reviews_json = json.loads(analyzed_reviews.to_json(orient="records"))
        logger.debug(f"Analyzed reviews JSON: {analyzed_reviews_json}")
    except Exception as e:
        # return {"status": 400, "message": str(e)}
        pass

    for i, review in enumerate(reviews_list):
        review.update(analyzed_reviews_json[i])
        logger.debug(f"Updated review with analysis: {review}")

    industry_id = INDUSTRY_MAP.get(industry, "001")
    for review in reviews_list:
        review["industry_id"] = industry_id
        logger.debug(f"Added industry_id to review: {review}")

    logger.info(
        f"Completed fetching and analyzing reviews for business_id: {business_id}"
    )
    return {"status": 200, "data": reviews_list}


def fetch_reviews_function(query, business_id, n_reviews, industry):
    return fetch_and_analyze_reviews(query, business_id, n_reviews, industry)


if __name__ == "__main__":
    fetch_and_analyze_reviews("", "pearls-deluxe-burgers-san-francisco-3", 10, "")
