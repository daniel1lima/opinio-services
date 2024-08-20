import json
import pandas as pd
from connectors.base_review import ReviewEntry
from models.models import ReviewModel
from modules.create_embeddings import analyze_reviews
from modules.logger_setup import setup_logger
from typing import List, Optional

logger = setup_logger()


class Analyzer:
    def __init__(self, connector):
        self.connector = connector
        self.table_name = "Reviews"  # Replace with your DynamoDB table name

    def initial_onboarding(self, config, n_reviews: int = 500):
        """
        Perform initial onboarding by fetching and analyzing historical reviews.

        Args:
            config: Configuration for the connector.
            n_reviews (int): Number of historical reviews to fetch. Defaults to 500.

        Returns:
            dict: A dictionary containing status and data or error message.
        """
        logger.info(
            f"Starting initial onboarding for config {config}, fetching {n_reviews} reviews"
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

        return self._process_reviews(reviews_list)

    def poll_new_reviews(self, config, last_sync: Optional[str] = ""):
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

        return self._process_reviews(reviews_list)

    def resume_fetch(self, config):
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

        result = self._process_reviews(reviews_list)
        result["total_fetched"] = total_fetched
        return result

    def _process_reviews(self, reviews_list: List[ReviewEntry]):
        """
        Process the fetched reviews: analyze them and save to DynamoDB.

        Args:
            reviews_list (List[ReviewEntry]): List of reviews to process.

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
        self.save_to_dynamodb(reviews_list)

        logger.info("Analysis and saving to DynamoDB completed.")
        return {"status": 200, "data": reviews_list}

    def save_to_dynamodb(self, reviews):
        for i, review in enumerate(reviews):
            try:
                review_dict = review.dict(exclude_unset=True)
                review_model = ReviewModel(
                    business_id=review_dict["business_id"],
                    company_id=review_dict["company_id"],
                    review_id=review_dict["review_id"],
                    review_date=review_dict["review_date"],
                    review_text=review_dict["review_text"],
                    review_url=review_dict["review_url"],
                    rating=int(review_dict["rating"]),
                    total_reviews=int(review_dict["total_reviews"]),
                    platform_id=review_dict["platform_id"],
                    assigned_label=review_dict.get("assigned_label"),
                    named_labels=review_dict.get("named_labels"),
                    sentiment=float(review_dict.get("sentiment", 0)),
                    polarity=float(review_dict.get("polarity", 0)),
                )
                review_model.save()
                logger.debug(f"Saved review to DynamoDB: {review_dict}")
            except Exception as e:
                logger.error(f"Error saving review number {i} to DynamoDB: {e}")


if __name__ == "__main__":
    from connectors.yelp import YelpConnector

    config = YelpConnector.yelpConfig(
        business_id="pearls-deluxe-burgers-san-francisco-3"
    )
    connector = YelpConnector(config)
    analyzer = Analyzer(connector)

    # Example usage:
    # Initial onboarding
    result = analyzer.initial_onboarding(config)
    print("Initial onboarding result:", result)

    # Poll for new reviews
    last_sync = "2023-04-01T00:00:00Z"
    result = analyzer.poll_new_reviews(config, last_sync)
    print("Poll new reviews result:", result)

    # Resume fetch
    result = analyzer.resume_fetch(config)
    print("Resume fetch result:", result)
