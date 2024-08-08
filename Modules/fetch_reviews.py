import json
import time
import pandas as pd
import requests
from pymongo import MongoClient


class BrokenReviewsApi(Exception):
    def __init__(self, message="An error occurred with the reviews API"):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message


def get_business_reviews(business_id, n_reviews, retries=3):
    url = "https://red-flower-business-data.p.rapidapi.com/business-reviews"
    headers = {
        "x-rapidapi-key": "52f0c4fb7cmsh68305c1877afa13p1710b0jsn6ea7e5079542",
        "x-rapidapi-host": "red-flower-business-data.p.rapidapi.com",
    }

    # MongoDB setup
    client = MongoClient("mongodb://localhost:27017/")
    db = client["reviews_db"]
    collection = db["reviews"]

    reviews_list = []
    page_size = 10
    total_fetched = 0
    page = 1

    while total_fetched < n_reviews:
        params = {
            "business_id": business_id,
            "page": page,
            "page_size": page_size,
            "num_pages": 1,
            "sort": "BEST_MATCH",
            "language": "en",
        }

        for attempt in range(retries):
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                try:
                    reviews_data = response.json()
                    data = reviews_data.get("data", {})
                    reviews = data.get("reviews", [])
                    total_reviews = data.get("total", 0)
                    business_name = "Unknown"

                    for review in reviews:
                        review_entry = {
                            "business_id": business_id,
                            "business_name": business_name,
                            "review_id": review.get("review_id", "Unknown"),
                            "review_date": review.get("review_datetime_utc", "Unknown"),
                            "review_text": review.get("review_text", ""),
                            "review_url": review.get("url", "No Url"),
                            "rating": review.get("review_rating", 0),
                            "total_reviews": total_reviews,
                            "platform_id": "001",
                        }
                        reviews_list.append(review_entry)
                        # Insert each review into MongoDB
                        collection.insert_one(review_entry)

                    total_fetched += len(reviews)
                    if len(reviews) < page_size:
                        break
                    page += 1
                    break
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                    return pd.DataFrame()
                except KeyError as e:
                    print(f"KeyError: {e}. Response JSON structure might have changed.")
                    return pd.DataFrame()
            elif response.status_code in [502, 503, 504]:
                print(
                    f"Server error {response.status_code}. Retrying... ({attempt + 1}/{retries})"
                )
                time.sleep(2**attempt)
            else:
                print(
                    f"Error fetching reviews: {response.status_code} - {response.reason}"
                )
                return pd.DataFrame()

    reviews_df = pd.DataFrame(reviews_list)
    return reviews_df


if __name__ == "__main__":
    BUSINESS_ID = "pearls-deluxe-burgers-san-francisco-3"
    N_REVIEWS = 10

    reviews_df = get_business_reviews(BUSINESS_ID, N_REVIEWS)
    reviews_df.to_csv("reviews.csv")
    print(reviews_df)


def single_business_scrape_reviews_function(request_data):
    return get_business_reviews(
        request_data.get("query", None),
        request_data.get("pages", None),
        request_data.get("business_count", None),
    )
