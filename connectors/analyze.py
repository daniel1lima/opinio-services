import json
import pandas as pd
from connectors.base_review import ReviewEntry
from models.models import InboxModel, ReviewModel
from modules.create_embeddings import analyze_reviews
from modules.logger_setup import setup_logger
from typing import List, Optional

logger = setup_logger("analyzer.log")


class Analyzer:
    def __init__(self, connector):
        self.connector = connector
        self.table_name = "Reviews"  # Replace with your DynamoDB table name

    def initial_onboarding(self, config, user_id, n_reviews: int = 300):
        """
        Perform initial onboarding by fetching and analyzing historical reviews.

        Args:
            config: Configuration for the connector.
            n_reviews (int): Number of historical reviews to fetch. Defaults to 500.

        Returns:
            dict: A dictionary containing status and data or error message.
        """
        logger.info(
            f"Starting initial onboarding for config {config}, fetching {n_reviews} reviews for user {user_id}"
        )

        try:
            reviews_list = self.connector.fetch_historical_reviews(n_reviews)
            if not reviews_list:
                return {
                    "status": 400,
                    "message": "Failed to fetch historical reviews, check logs / check business_id",
                }
        except Exception as e:
            logger.error("Failed to fetch historical reviews.", exc_info=True)
            return {"status": 400, "message": "Failed to fetch historical reviews."}

        return self._process_reviews(reviews_list, user_id)

    def poll_new_reviews(self, config, user_id, last_sync: Optional[str] = ""):
        """
        Poll for new reviews since the last sync.

        Args:
            config: Configuration for the connector.
            last_sync (Optional[str]): The UTC datetime of the last sync.

        Returns:
            dict: A dictionary containing status and data or error message.
        """
        logger.info(f"Polling for new reviews for config {config} since {last_sync}")

        try:
            reviews_list = self.connector.fetch_new_reviews(last_sync)
            if not reviews_list:
                return {
                    "status": 200,
                    "message": "No new reviews found.",
                }
        except Exception as e:
            logger.error("Failed to fetch new reviews.", exc_info=True)
            return {"status": 400, "message": "Failed to fetch new reviews."}

        return self._process_reviews(reviews_list, user_id)

    def resume_fetch(self, config, user_id):
        """
        Resume fetching reviews from the last saved progress.

        Args:
            config: Configuration for the connector.

        Returns:
            dict: A dictionary containing status and data or error message.
        """
        logger.info(f"Resuming fetch for config {config}")

        try:
            reviews_list, total_fetched = self.connector.resume_fetch(
                config.business_id
            )
            if not reviews_list:
                return {
                    "status": 200,
                    "message": f"No new reviews found. Total fetched: {total_fetched}",
                }
        except Exception as e:
            logger.error("Failed to resume fetching reviews.", exc_info=True)
            return {"status": 400, "message": "Failed to resume fetching reviews."}

        result = self._process_reviews(reviews_list, user_id)
        result["total_fetched"] = total_fetched
        return result

    def _process_reviews(self, reviews_list: List[ReviewEntry], user_id: str):
        """
        Process the fetched reviews: analyze them and save to DynamoDB.

        Args:
            reviews_list (List[ReviewEntry]): List of reviews to process.
            user_id (str): The user ID associated with the reviews.
        Returns:
            dict: A dictionary containing status and processed reviews.
        """
        reviews_text = [review.review_text for review in reviews_list]

        # Analyze the reviews
        analyzed_reviews, summaries = analyze_reviews(reviews_text)

        # Update the original reviews with analysis results
        for i, review in enumerate(reviews_list):
            for key, value in analyzed_reviews[i].items():
                if not hasattr(review, key):
                    setattr(review, key, value)

        # Save analyzed reviews to DynamoDB
        self.save_to_dynamodb(reviews_list, user_id)

        return {"status": 200, "data": reviews_list}

    def save_to_dynamodb(self, reviews, user_id):
        for i, review in enumerate(reviews):
            try:
                review_dict = review.dict(exclude_unset=True)
                review_model = ReviewModel(
                    business_id=review_dict["business_id"],
                    company_id=review_dict["company_id"],
                    review_date=review_dict["review_date"],
                    review_id=review_dict["review_id"],
                    review_text=review_dict["review_text"],
                    review_url=review_dict.get("review_url", ""),
                    rating=int(review_dict.get("rating", 0)),
                    total_reviews=int(review_dict.get("total_reviews", 0)),
                    platform_id=review_dict.get("platform_id", ""),
                    assigned_label=review_dict.get("assigned_label", ""),
                    named_labels=review_dict.get("named_labels", []),
                    sentiment=float(review_dict.get("sentiment", 0.0)),
                    polarity=float(review_dict.get("polarity", 0.0)),
                    author_name=review_dict.get("author_name", "Anonymous"),
                    author_image_url=review_dict.get("author_image_url", ""),
                )
                review_model.save()

                # Save to inbox
                InboxModel.create_inbox_item(
                    user_id=user_id, review=review
                )  # Create inbox review for each company review
            except Exception as e:
                logger.error(f"Error saving review number {i} to DynamoDB: {e}")
