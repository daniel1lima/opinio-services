import json
import os
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from pydantic import ValidationError

from connectors.base_review import ReviewEntry
from connectors.publish import publish_job_status
from models.models import CompanyModel, JobModel, JobStatus
from modules.logger_setup import setup_logger

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType


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
        start_offset: int = 0,
    ) -> List[ReviewEntry]:
        """
        Fetches reviews from Yelp for a given business using web scraping.

        Args:
            last_sync (Optional[str]): The UTC datetime of the last sync. If None or empty, fetches all reviews.
            n_reviews (int, optional): The maximum number of reviews to fetch. Defaults to infinity.
            start_offset (int, optional): The offset to start fetching reviews from. Defaults to 0.

        Returns:
            List[ReviewEntry]: A list of validated review entries.
        """
        self.job.update_status(JobStatus.IN_PROGRESS.value)

        reviews_list = []
        total_fetched = 0
        fetch_completed = True

        last_sync_dt = self._parse_last_sync(last_sync)
        self.logger.info(
            f"Fetching reviews for business_id: {self.business_id}, company_id: {self.company_id}, last_sync: {last_sync_dt}, n_reviews: {n_reviews}, start_offset: {start_offset}"
        )

        try:
            while total_fetched < n_reviews:
                new_reviews = self._scrape_reviews_page(start_offset + total_fetched)

                if not new_reviews:
                    self.logger.info("No new reviews found. Stopping fetch.")
                    break

                filtered_reviews = self._filter_reviews(new_reviews, last_sync_dt)
                reviews_list.extend(filtered_reviews)
                total_fetched += len(filtered_reviews)
                self.logger.info(
                    f"Fetched {len(filtered_reviews)} new reviews, total fetched: {total_fetched}"
                )

                self._save_progress(
                    self.business_id, total_fetched + start_offset, last_sync
                )
                self.job.update_status(
                    JobStatus.IN_PROGRESS.value, total_reviews_fetched=total_fetched
                )

                if (
                    len(filtered_reviews) < len(new_reviews)
                    or total_fetched >= n_reviews
                ):
                    break

            reviews_list = reviews_list[:n_reviews]

            if reviews_list:
                latest_review_date = max(review.review_date for review in reviews_list)
                self._update_last_sync(latest_review_date)

                self.job.update_status(
                    JobStatus.COMPLETED.value,
                    total_reviews_fetched=len(reviews_list),
                    last_sync=latest_review_date,
                )
            else:
                self.logger.warning("No reviews found.")
                self.job.update_status(
                    JobStatus.FAILED.value, error_message="No reviews found."
                )
                publish_job_status(
                    self.company_id,
                    {
                        "job_id": self.job_id,
                        "status": JobStatus.FAILED.value,
                        "error_message": "No reviews found.",
                    },
                )

            return reviews_list
        except Exception as e:
            self.logger.error(f"Error fetching reviews: {str(e)}")
            self.job.update_status(JobStatus.FAILED.value, error_message=str(e))
            return []

    def _scrape_reviews_page(self, offset: int) -> List[dict]:
        """
        Scrapes a single page of Yelp reviews.

        Args:
            offset (int): The offset to start scraping from.

        Returns:
            List[dict]: A list of raw review data dictionaries.
        """

        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
        }
        links_with_text = []
        final_city_links = []
        info_scraped = {}

        url = f"https://www.yelp.com/biz/{self.business_id}?start={offset}"
        print(url)
        self.logger.info(f"Scraping reviews from URL: {url}")

        # Initialize the WebDriver (assuming Chrome is being used)
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # Run in headless mode
        driver = webdriver.Chrome(options=options)

        try:
            driver.get(url)
            soup = BeautifulSoup(driver.page_source, "html.parser")

            # Find review elements on the page
            review_elements = soup.find_all(
                "div", class_="review__373c0__13kpL"
            )  # Adjust class name as needed
            for review in review_elements:
                review_data = {
                    "review_id": review.get("data-review-id"),
                    "review_datetime_utc": review.find("time")["datetime"],
                    "review_text": review.find(
                        "p", class_="comment__373c0__Ns8jV"
                    ).get_text(strip=True),
                    "review_rating": review.find(
                        "span", class_="display--inline__373c0__2SfH_"
                    ).get_text(strip=True),
                    "author_name": review.find("span", class_="css-1x6d8g2").get_text(
                        strip=True
                    ),
                    "author_image_url": review.find("img", class_="css-1s8z0g8")["src"]
                    if review.find("img", class_="css-1s8z0g8")
                    else None,
                    "total_reviews": int(
                        soup.find("span", class_="css-1e4fdj9")
                        .get_text(strip=True)
                        .split()[0]
                    ),  # Total reviews count
                }
                links_with_text.append(review_data)

        finally:
            driver.quit()

        return links_with_text

        # TODO: Implement web scraping logic here
        # This function should return a list of dictionaries containing raw review data
        pass

    def _filter_reviews(
        self, reviews: List[dict], last_sync_dt: Optional[datetime]
    ) -> List[ReviewEntry]:
        """
        Filters and converts raw review data to ReviewEntry objects.

        Args:
            reviews (List[dict]): List of raw review data dictionaries.
            last_sync_dt (Optional[datetime]): The last sync datetime to filter reviews.

        Returns:
            List[ReviewEntry]: A list of filtered and validated ReviewEntry objects.
        """
        filtered_reviews = []
        for review in reviews:
            review_dt = datetime.fromisoformat(
                review.get("review_datetime_utc")
            ).replace(tzinfo=timezone.utc)
            if last_sync_dt and review_dt <= last_sync_dt:
                continue

            try:
                review_entry = ReviewEntry(
                    business_id=self.business_id,
                    company_id=self.company_id,
                    review_id=review.get("review_id"),
                    review_date=review_dt.isoformat(),
                    review_text=review.get("review_text", ""),
                    review_url=f"https://www.yelp.com/biz/{self.business_id}?hrid={review.get('review_id')}",
                    rating=float(review.get("review_rating", 0)),
                    total_reviews=review.get("total_reviews", 0),
                    platform_id="Yelp",
                    author_name=review.get("author_name") or "Anonymous",
                    author_image_url=review.get("author_image_url") or "No Url",
                )
                filtered_reviews.append(review_entry)
            except ValidationError as ve:
                self.logger.warning(f"Skipping invalid review: {ve}")

        return filtered_reviews

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

    def _save_progress(
        self, business_id: str, total_fetched: int, last_sync: Optional[str]
    ):
        progress = {
            "business_id": business_id,
            "total_fetched": total_fetched,
            "last_sync": last_sync,
        }
        with open(f"progress/progress_{business_id}.json", "w") as f:
            json.dump(progress, f)

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


if __name__ == "__main__":
    connector = YelpConnector()
    connector._scrape_reviews_page(0)
