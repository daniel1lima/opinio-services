import json
import os
import time
import requests
from modules.fetch_reviews import Review
from modules.logger_setup import setup_logger
from pydantic import BaseModel, Field, ValidationError
from connectors.base_review import ReviewEntry
from typing import List, Optional, Tuple
from datetime import datetime, timezone
from models.models import CompanyModel

from dotenv import load_dotenv

load_dotenv()


class YelpConnector:
    class yelpConfig:
        business_id: str
        company_id: str

    def __init__(self, config) -> None:
        self.business_id = config.business_id
        self.company_id = config.company_id
        self.logger = setup_logger("yelp_connector.log")

    def fetch_historical_reviews(self, n_reviews: int = 500) -> List[ReviewEntry]:
        """
        Fetches the last n_reviews for the specified business.

        Args:
            n_reviews (int): The number of historical reviews to fetch. Defaults to 500.

        Returns:
            List[ReviewEntry]: A list of validated review entries.
        """
        return self.fetch_reviews(self.business_id, last_sync=None, n_reviews=n_reviews)

    def fetch_new_reviews(self, last_sync: Optional[str]) -> List[ReviewEntry]:
        """
        Fetches new reviews for the specified business since the last sync.

        Args:
            last_sync (Optional[str]): The UTC datetime of the last sync.

        Returns:
            List[ReviewEntry]: A list of validated new review entries.
        """
        try:
            return self.fetch_reviews(
                self.business_id, last_sync, n_reviews=float("inf")
            )
        except Exception as e:
            self.logger.error(f"Error fetching new reviews: {e}")
            return []

    def fetch_reviews(
        self,
        business_id: str,
        last_sync: Optional[str],
        n_reviews: int = float("inf"),
        max_retries: int = 5,
        initial_backoff: float = 1.0,
        start_offset: int = 0,
    ) -> List[ReviewEntry]:
        """
        Fetches reviews from the Yelp API for a given business.

        Args:
            business_id (str): The ID of the business to fetch reviews for.
            last_sync (Optional[str]): The UTC datetime of the last sync. If None or empty, fetches all reviews.
            n_reviews (int, optional): The maximum number of reviews to fetch. Defaults to infinity.
            max_retries (int, optional): The maximum number of retry attempts for failed requests. Defaults to 5.
            initial_backoff (float, optional): The initial backoff time in seconds. Defaults to 1.0.
            start_offset (int, optional): The offset to start fetching reviews from. Defaults to 0.

        Returns:
            List[ReviewEntry]: A list of validated review entries.
        """
        url = "https://red-flower-business-data.p.rapidapi.com/business-reviews"
        headers = {
            "x-rapidapi-key": os.environ["RAPIDAPI_KEY"],
            "x-rapidapi-host": "red-flower-business-data.p.rapidapi.com",
        }

        reviews_list = []
        page_size = 4
        total_fetched = 0
        page = (start_offset // page_size) + 1

        last_sync_dt = self._parse_last_sync(last_sync)
        self.logger.info(
            f"Fetching reviews for business_id: {business_id}, last_sync: {last_sync_dt}, n_reviews: {n_reviews}, start_offset: {start_offset}"
        )

        while total_fetched < n_reviews:
            params = self._build_request_params(business_id, page, page_size)
            response = self._make_api_request(
                url, headers, params, max_retries, initial_backoff
            )

            if not response:
                self.logger.warning(
                    f"Failed to fetch page {page}. Returning {len(reviews_list)} reviews processed so far."
                )
                return reviews_list

            new_reviews = self._process_response(response, business_id, last_sync_dt)
            if not new_reviews:
                self.logger.info(
                    f"No new reviews found on page {page}. Stopping fetch."
                )
                break

            reviews_list.extend(new_reviews)
            total_fetched += len(new_reviews)
            self.logger.info(
                f"Fetched {len(new_reviews)} new reviews, total fetched: {total_fetched}"
            )

            self._save_progress(business_id, total_fetched + start_offset, last_sync)

            if len(new_reviews) < page_size or total_fetched >= n_reviews:
                break
            page += 1

        reviews_list = reviews_list[:n_reviews]

        # Update the last_sync for this connector in the company
        if reviews_list:
            latest_review_date = max(review.review_date for review in reviews_list)
            self._update_last_sync(self.company_id, latest_review_date)

        return reviews_list

    def _save_progress(
        self, business_id: str, total_fetched: int, last_sync: Optional[str]
    ):
        """
        Saves the progress of the review fetching process.

        Args:
            business_id (str): The ID of the business.
            total_fetched (int): The total number of reviews fetched so far.
            last_sync (Optional[str]): The last sync datetime.
        """
        progress = {
            "business_id": business_id,
            "total_fetched": total_fetched,
            "last_sync": last_sync,
        }
        with open(f"progress_{business_id}.json", "w") as f:
            json.dump(progress, f)

    def resume_fetch(self, business_id: str) -> Tuple[List[ReviewEntry], int]:
        """
        Resumes fetching reviews from the last saved progress.

        Args:
            business_id (str): The ID of the business to resume fetching for.

        Returns:
            Tuple[List[ReviewEntry], int]: A tuple containing the list of fetched reviews and the total number of reviews fetched.
        """
        try:
            with open(f"progress_{business_id}.json", "r") as f:
                progress = json.load(f)

            start_offset = progress["total_fetched"]
            last_sync = progress["last_sync"]

            # add n_reviews if only resuming by a certain amount not fetching all reviews
            new_reviews = self.fetch_reviews(
                business_id, last_sync, start_offset=start_offset
            )
            total_fetched = start_offset + len(new_reviews)

            return new_reviews, total_fetched
        except FileNotFoundError:
            self.logger.warning(
                f"No progress file found for business_id: {business_id}. Starting from the beginning."
            )
            return self.fetch_historical_reviews(), 0

    def _parse_last_sync(self, last_sync: Optional[str]) -> Optional[datetime]:
        if not last_sync:
            self.logger.info("No last sync date provided, fetching all reviews.")
            return None
        try:
            return datetime.fromisoformat(last_sync).replace(tzinfo=timezone.utc)
        except ValueError:
            self.logger.warning(
                f"Invalid last_sync format: {last_sync}. Fetching all reviews."
            )
            return None

    def _build_request_params(
        self, business_id: str, page: int, page_size: int
    ) -> dict:
        return {
            "business_id": business_id,
            "page": page,
            "page_size": page_size,
            "num_pages": 1,
            "sort": "NEWEST",
            "language": "en",
        }

    def _make_api_request(
        self,
        url: str,
        headers: dict,
        params: dict,
        max_retries: int,
        initial_backoff: float,
    ) -> Optional[requests.Response]:
        backoff = initial_backoff
        for attempt in range(max_retries):
            self.logger.info(
                f"Fetching page {params['page']} (attempt {attempt + 1}/{max_retries})"
            )
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10)
                self.logger.warning(f"response: {response}")
                response.raise_for_status()

                return response
            except requests.RequestException as e:
                self.logger.warning(
                    f"Request failed: {e}. Retrying in {backoff:.2f} seconds... (attempt {attempt + 1}/{max_retries})"
                )
                if attempt == max_retries - 1:
                    self.logger.error(
                        f"Failed to fetch reviews after {max_retries} attempts."
                    )
                    return None
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff

        return None

    def _process_response(
        self,
        response: requests.Response,
        business_id: str,
        last_sync_dt: Optional[datetime],
    ) -> List[ReviewEntry]:
        try:
            reviews_data = response.json()
            data = reviews_data.get("data", {})
            reviews = data.get("reviews", [])
            total_reviews = data.get("total", 0)

            new_reviews = []
            for review in reviews:
                review_dt = datetime.fromisoformat(
                    review.get("review_datetime_utc")
                ).replace(tzinfo=timezone.utc)
                if last_sync_dt and review_dt <= last_sync_dt:
                    continue

                try:
                    review_entry = ReviewEntry(
                        business_id=business_id,
                        company_id=business_id,
                        review_id=review.get("review_id", "Unknown"),
                        review_date=review_dt.isoformat(),  # Convert datetime to ISO format string
                        review_text=review.get("review_text", ""),
                        review_url=review.get("url", "No Url"),
                        rating=float(
                            review.get("review_rating", 0)
                        ),  # Ensure rating is a float
                        total_reviews=total_reviews,
                        platform_id="Yelp",
                    )
                    new_reviews.append(review_entry)
                except ValidationError as ve:
                    self.logger.warning(f"Skipping invalid review: {ve}")

            return new_reviews
        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"Error processing response: {e}")
            return []

    def _update_last_sync(self, company_id: str, latest_review_date: str):
        try:
            company = CompanyModel.get_company_by_id(company_id)
            if company:
                for connector in company.connectors:
                    if connector.type == "Yelp":
                        connector.last_sync = latest_review_date
                        break
                company.save()
                self.logger.info(
                    f"Updated last_sync for Yelp connector to {latest_review_date}"
                )
            else:
                self.logger.warning(f"Company with id {company_id} not found")
        except Exception as e:
            self.logger.error(f"Failed to update last_sync: {e}")
