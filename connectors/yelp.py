import json
import os
import time
import requests
from modules.fetch_reviews import Review
from modules.logger_setup import setup_logger
from pydantic import BaseModel, Field, ValidationError
from base_review import ReviewEntry

from dotenv import load_dotenv

load_dotenv()


class YelpConnector:
    class yelpConfig:
        businessId: str

    def __init__(self, config) -> None:
        self.business_id = config.business_id
        self.logger = setup_logger("yelp_connector.log")

    def fetch_new_reviews(self, last_sync) -> list:
        """
        Fetches new reviews for the specified business.

        Returns:
            list: A list of validated review entries. If an error occurs, returns an empty list.
        """
        try:
            reviews = self.fetch_reviews(self.business_id, last_sync)
            return reviews
        except Exception as e:
            self.logger.error(f"Error fetching new reviews: {e}")
            return []

    # float('inf')
    def fetch_reviews(
        self, business_id, last_sync, n_reviews=8, retries=3
    ) -> list[ReviewEntry]:
        """
        Fetches reviews from the Yelp API for a given business.

        Args:
            business_id (str): The ID of the business to fetch reviews for.
            n_reviews (int, optional): The maximum number of reviews to fetch. Defaults to infinity.
            retries (int, optional): The number of retry attempts for failed requests. Defaults to 3.

        Returns:
            list: A list of validated review entries. If an error occurs, returns an empty list.
        """
        url = "https://red-flower-business-data.p.rapidapi.com/business-reviews"
        headers = {
            "x-rapidapi-key": os.environ["RAPIDAPI_KEY"],
            "x-rapidapi-host": "red-flower-business-data.p.rapidapi.com",
        }

        reviews_list = []
        page_size = 4
        total_fetched = 0
        page = 1

        self.logger.info(
            f"Starting to fetch reviews for business_id: {business_id}, n_reviews: {n_reviews}"
        )

        # Check if n_reviews is finite
        if not (isinstance(n_reviews, (int, float)) and n_reviews > 0):
            n_reviews = float(
                "inf"
            )  # Default to infinity if not a valid positive number

        while total_fetched < n_reviews:
            params = {
                "business_id": business_id,
                "page": page,
                "page_size": page_size,
                "num_pages": 1,
                "sort": "NEWEST",
                "language": "en",
            }

            for attempt in range(retries):
                self.logger.info(
                    f"Fetching page {page} (attempt {attempt + 1}) with params: {params}"
                )
                response = requests.get(url, headers=headers, params=params)
                self.logger.debug(f"Response status code: {response.status_code}")
                if response.status_code == 200:
                    try:
                        reviews_data = response.json()
                        self.logger.debug(f"Response JSON: {reviews_data}")
                        data = reviews_data.get("data", {})
                        reviews = data.get("reviews", [])

                        # Handle case where last_sync is an empty string
                        if last_sync == "":
                            self.logger.info(
                                "No last sync date provided, fetching all reviews."
                            )
                        else:
                            # Filter reviews based on last_sync
                            reviews = [
                                review
                                for review in reviews
                                if review.get("review_datetime_utc") > last_sync
                            ]

                        # Stop fetching if no new reviews are found
                        if not reviews:
                            self.logger.info("No new reviews since last sync, ending.")
                            break

                        total_reviews = data.get("total", 0)

                        reviews_list.extend(
                            [
                                ReviewEntry(
                                    business_id=business_id,
                                    company_id=business_id,
                                    review_id=review.get("review_id", "Unknown"),
                                    review_date=review.get("review_datetime_utc", None),
                                    review_text=review.get("review_text", ""),
                                    review_url=review.get("url", "No Url"),
                                    rating=review.get("review_rating", 0),
                                    total_reviews=total_reviews,
                                    platform_id="Yelp",
                                )
                                for review in reviews
                                if (
                                    entry := ReviewEntry(
                                        business_id=business_id,
                                        company_id=business_id,
                                        review_id=review.get("review_id", "Unknown"),
                                        review_date=review.get(
                                            "review_datetime_utc", None
                                        ),
                                        review_text=review.get("review_text", ""),
                                        review_url=review.get("url", "No Url"),
                                        rating=review.get("review_rating", 0),
                                        total_reviews=total_reviews,
                                        platform_id="Yelp",
                                    )
                                ).model_validate(
                                    entry
                                )  # Pass the instance to model_validate
                            ]
                        )
                        self.logger.debug(f"Validated reviews: {reviews_list}")

                        total_fetched += len(reviews)
                        self.logger.info(
                            f"Fetched {len(reviews)} reviews, total fetched: {total_fetched}"
                        )

                        if len(reviews) < page_size or total_fetched >= n_reviews:
                            self.logger.info("No more reviews to fetch, ending.")
                            break
                        page += 1
                        break
                    except json.JSONDecodeError as e:
                        self.logger.error(f"JSON decode error: {e}")
                        return []
                    except KeyError as e:
                        self.logger.error(
                            f"KeyError: {e}. Response JSON structure might have changed."
                        )
                        return []
                elif response.status_code in [502, 503, 504]:
                    self.logger.warning(
                        f"Server error {response.status_code}. Retrying... ({attempt + 1}/{retries})"
                    )
                    time.sleep(2**attempt)
                else:
                    self.logger.error(
                        f"Error fetching reviews: {response.status_code} - {response.text}"
                    )
                    return []

        return reviews_list
