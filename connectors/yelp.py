import json
import os
import time
import requests
from connectors.publish import publish_job_status
from modules.fetch_reviews import Review
from modules.logger_setup import setup_logger
from pydantic import BaseModel, Field, ValidationError
from connectors.base_review import ReviewEntry
from typing import List, Optional, Tuple
from datetime import datetime, timezone
from models.models import CompanyModel, JobModel, JobStatus

from dotenv import load_dotenv

load_dotenv(override=True)


class YelpConnector:
    def __init__(self, config) -> None:
        self.business_id = config["business_id"]
        self.company_id = config["company_id"]
        self.job_id = config["job_id"]
        self.logger = setup_logger(log_dir="logs/yelp_connector")
        self.job = JobModel.get(self.company_id)

    def fetch_historical_reviews(self, n_reviews: int = 500) -> List[ReviewEntry]:
        """
        Fetches the last n_reviews for the specified business.

        Args:
            n_reviews (int): The number of historical reviews to fetch. Defaults to 500.

        Returns:
            List[ReviewEntry]: A list of validated review entries.
        """
        return self.fetch_reviews(last_sync=None, n_reviews=n_reviews)

    def fetch_new_reviews(self, last_sync: Optional[str]) -> List[ReviewEntry]:
        """
        Fetches new reviews for the specified business since the last sync.

        Args:
            last_sync (Optional[str]): The UTC datetime of the last sync.

        Returns:
            List[ReviewEntry]: A list of validated new review entries.
        """
        try:
            return self.fetch_reviews(last_sync, n_reviews=float("inf"))
        except Exception as e:
            self.logger.error(f"Error fetching new reviews: {e}")
            return []

    def fetch_reviews(
        self,
        last_sync: Optional[str],
        n_reviews: int = float("inf"),
        max_retries: int = 5,
        initial_backoff: float = 1.0,
        start_offset: int = 0,
    ) -> List[ReviewEntry]:
        """
        Fetches reviews from the Yelp API for a given business.

        Args:
            last_sync (Optional[str]): The UTC datetime of the last sync. If None or empty, fetches all reviews.
            n_reviews (int, optional): The maximum number of reviews to fetch. Defaults to infinity.
            max_retries (int, optional): The maximum number of retry attempts for failed requests. Defaults to 5.
            initial_backoff (float, optional): The initial backoff time in seconds. Defaults to 1.0.
            start_offset (int, optional): The offset to start fetching reviews from. Defaults to 0.

        Returns:
            List[ReviewEntry]: A list of validated review entries.
        """
        self.job.update_status(JobStatus.IN_PROGRESS.value)

        url = "https://red-flower-business-data.p.rapidapi.com/business-reviews"
        headers = {
            "x-rapidapi-key": os.environ["RAPIDAPI_KEY"],
            "x-rapidapi-host": "red-flower-business-data.p.rapidapi.com",
        }

        reviews_list = []
        page_size = 45
        total_fetched = 0
        page = (start_offset // page_size) + 1

        last_sync_dt = self._parse_last_sync(last_sync)
        self.logger.info(
            f"Fetching reviews for business_id: {self.business_id}, company_id: {self.company_id}, last_sync: {last_sync_dt}, n_reviews: {n_reviews}, start_offset: {start_offset}"
        )

        fetch_completed = True  # Flag to track if fetch process completed successfully

        num_pages = 5  # Number of pages to fetch at a time

        try:
            while total_fetched < n_reviews:
                params = self._build_request_params(
                    self.business_id, page, page_size, num_pages
                )
                response = self._make_api_request(
                    url, headers, params, max_retries, initial_backoff
                )

                if not response:
                    self.logger.error(
                        f"Failed to fetch page {page} after {max_retries} attempts. Stopping fetch."
                    )
                    fetch_completed = False
                    break

                new_reviews = self._process_response(
                    response, self.company_id, last_sync_dt
                )
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

                self._save_progress(
                    self.business_id, total_fetched + start_offset, last_sync
                )
                self.job.update_status(
                    JobStatus.IN_PROGRESS.value, total_reviews_fetched=total_fetched
                )

                if (
                    len(new_reviews) < page_size * num_pages
                    or total_fetched >= n_reviews
                ):
                    break
                page += num_pages

            reviews_list = reviews_list[:n_reviews]

            if reviews_list:
                latest_review_date = max(review.review_date for review in reviews_list)
                self._update_last_sync(latest_review_date)

                if fetch_completed:
                    self.job.update_status(
                        JobStatus.COMPLETED.value,
                        total_reviews_fetched=len(reviews_list),
                        last_sync=latest_review_date,
                    )
                else:
                    self.job.update_status(
                        JobStatus.FAILED.value,
                        total_reviews_fetched=len(reviews_list),
                        last_sync=latest_review_date,
                        error_message="Fetch process interrupted due to API failures.",
                    )
                    publish_job_status(
                        self.company_id,
                        {
                            "job_id": self.job_id,
                            "status": JobStatus.FAILED.value,
                            "total_reviews_fetched": len(reviews_list),
                            "last_sync": latest_review_date,
                            "error_message": "Fetch process interrupted due to API failures.",
                        },
                    )
            else:
                self.logger.warning("No reviews found or all requests failed.")
                self.job.update_status(
                    JobStatus.FAILED.value,
                    error_message="No reviews found or all requests failed.",
                )
                publish_job_status(
                    self.company_id,
                    {
                        "job_id": self.job_id,
                        "status": JobStatus.FAILED.value,
                        "error_message": "No reviews found or all requests failed.",
                    },
                )

            return reviews_list
        except Exception as e:
            self.logger.error(f"Error fetching reviews: {str(e)}")
            self.job.update_status(JobStatus.FAILED.value, error_message=str(e))
            return []

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
        with open(f"progress/progress_{business_id}.json", "w") as f:
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
            with open(f"progress/progress_{business_id}.json", "r") as f:
                progress = json.load(f)

            start_offset = progress["total_fetched"]
            last_sync = progress["last_sync"]

            # add n_reviews if only resuming by a certain amount not fetching all reviews
            new_reviews = self.fetch_reviews(last_sync, start_offset=start_offset)
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
        self, business_id: str, page: int, page_size: int, num_pages: int
    ) -> dict:
        return {
            "business_id": business_id,
            "page": page,
            "page_size": page_size,
            "num_pages": num_pages,
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
                    self.job.update_status(
                        JobStatus.FAILED.value,
                        error_message=f"Failed to fetch reviews after {max_retries} attempts: {str(e)}",
                    )
                    publish_job_status(
                        self.company_id,
                        {
                            "job_id": self.job_id,
                            "status": JobStatus.FAILED.value,
                            "error_message": f"Failed to fetch reviews after {max_retries} attempts: {str(e)}",
                        },
                    )
                    return None
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff

        return None

    def _process_response(
        self,
        response: requests.Response,
        company_id: str,
        last_sync_dt: Optional[datetime],
    ) -> List[ReviewEntry]:
        try:
            reviews_data = response.json()
            data = reviews_data.get("data", {})
            reviews = data.get("reviews", [])
            total_reviews = data.get("total", 0)

            # Debug logging to check the type and content of reviews
            self.logger.debug(f"Type of reviews: {type(reviews)}, Content: {reviews}")

            if not isinstance(reviews, list):
                self.logger.error(f"Expected list of reviews, got {type(reviews)}")
                return []

            new_reviews = []
            for review in reviews:
                review_dt = datetime.fromisoformat(
                    review.get("review_datetime_utc")
                ).replace(tzinfo=timezone.utc)
                if last_sync_dt and review_dt <= last_sync_dt:
                    continue

                try:
                    review_entry = ReviewEntry(
                        business_id=self.business_id,
                        company_id=company_id,
                        review_id=review.get("review_id"),
                        review_date=review_dt.isoformat(),  # Convert datetime to ISO format string
                        review_text=review.get("review_text", ""),
                        review_url=f"https://www.yelp.com/biz/{self.business_id}?hrid={review.get('review_id')}&utm_campaign=www_review_share_popup&utm_medium=copy_link&utm_source=(direct)",
                        rating=float(
                            review.get("review_rating", 0)
                        ),  # Ensure rating is a float
                        total_reviews=total_reviews,
                        platform_id="Yelp",
                        author_name=review.get("author_name") or "Anonymous",
                        author_image_url=review.get("author_image_url") or "No Url",
                    )
                    new_reviews.append(review_entry)
                except ValidationError as ve:
                    self.logger.warning(review)
                    self.logger.warning(f"Skipping invalid review: {ve}")

            return new_reviews
        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"Error processing response: {e}")
            return []

    def _update_last_sync(self, latest_review_date: str):
        try:
            company = CompanyModel.get_company_by_id(self.company_id)
            if company:
                for connector in company.connectors:
                    if (
                        connector.type == "Yelp"
                        and connector.config["business_id"] == self.business_id
                    ):
                        connector.last_sync = latest_review_date
                        break
                company.save()
                self.logger.info(
                    f"Updated last_sync for Yelp connector (business_id: {self.business_id}) to {latest_review_date}"
                )
            else:
                self.logger.warning(f"Company with id {self.company_id} not found")
        except Exception as e:
            self.logger.error(f"Failed to update last_sync: {e}")
