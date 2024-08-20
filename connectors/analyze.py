import json

import pandas as pd
from models.models import ReviewModel
from modules.create_embeddings import analyze_reviews
from modules.logger_setup import setup_logger

logger = setup_logger()


class Analyzer:
    def __init__(self, connector):
        self.connector = connector
        self.table_name = "Reviews"  # Replace with your DynamoDB table name

    def analyze_reviews(self, config, last_sync=""):
        logger.info(f"Starting analysis for config {config} and last_sync {last_sync}")

        # Fetch reviews using the connector's fetch_reviews function
        try:
            response = self.connector.fetch_new_reviews(
                last_sync
            )  # Adjusted to match method signature
            if not response:
                return {
                    "status": 400,
                    "message": "Failed to fetch reviews, check logs / check business_id",
                }
        except Exception as e:
            logger.error("Failed to fetch reviews.", exc_info=True)
            return {"status": 400, "message": "Failed to fetch reviews."}

        # here

        reviews_list = response
        reviews_text = [review.review_text for review in reviews_list]

        # Analyze the reviews
        analyzed_reviews, summaries = analyze_reviews(reviews_text)

        # Update the original reviews with analysis results
        for i, review in enumerate(reviews_list):
            # Add any additional attributes from analyzed_reviews that are not in ReviewEntry
            for key, value in analyzed_reviews[i].items():
                if not hasattr(review, key):  # Check if the attribute already exists
                    setattr(review, key, value)  # Dynamically add the new attribute

        # Save analyzed reviews to DynamoDB
        self.save_to_dynamodb(reviews_list)

        logger.info("Analysis and saving to DynamoDB completed.")
        return {"status": 200, "data": reviews_list}

    def save_to_dynamodb(self, reviews):
        for i, review in enumerate(reviews):
            try:
                # Convert ReviewEntry instance to a dictionary
                review_dict = review.dict(
                    exclude_unset=True
                )  # Exclude unset fields if necessary
                # Manually assign each field to the ReviewModel
                review_model = ReviewModel(
                    business_id=review_dict["business_id"],
                    company_id=review_dict["company_id"],
                    review_id=review_dict["review_id"],
                    review_date=review_dict["review_date"],
                    review_text=review_dict["review_text"],
                    review_url=review_dict["review_url"],
                    rating=int(review_dict["rating"]),  # Convert to int
                    total_reviews=int(review_dict["total_reviews"]),  # Convert to int
                    platform_id=review_dict["platform_id"],
                    assigned_label=review_dict["assigned_label"],
                    named_labels=review_dict["named_labels"],
                    sentiment=float(review_dict["sentiment"]),
                    polarity=float(review_dict["polarity"]),  # Convert to float
                )
                review_model.save()  # Save to DynamoDB
                logger.debug(f"Saved review to DynamoDB: {review_dict}")
            except Exception as e:
                logger.error(f"Error saving review number {i} to DynamoDB: {e}")


if __name__ == "__main__":
    from modules.fetch_reviews import (
        fetch_reviews_function,
    )  # Import your fetch function

    class SampleConnector:
        def fetch_reviews(self, business_id, n_reviews, industry):
            return fetch_reviews_function(business_id, n_reviews, industry)

    connector = SampleConnector()
    analyzer = Analyzer(connector)
    result = analyzer.analyze_reviews("pearls-deluxe-burgers-san-francisco-3", 10, "")
    print(result)
