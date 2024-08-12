def get_business_reviews(api_key, business_id, n_reviews, retries=3):
    url = "https://yelp-reviews.p.rapidapi.com/business-reviews"
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': "yelp-reviews.p.rapidapi.com",
        'Content-Type': "application/json"
    }
    
    reviews_list = []
    page_size = 20 ## change to params that could be decided on api call
    total_fetched = 0
    page = 1

    while total_fetched < n_reviews:
        params = {
            "business_id": business_id,
            "page": page,
            "page_size": page_size,
            "num_pages": 1,
            "language": "en"
        }
        
        for attempt in range(retries):
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                try:
                    reviews_data = response.json()

                    # Debugging: print the response to inspect its structure
                    print("Response JSON:", reviews_data)

                    business_name = reviews_data.get('business_name', 'Unknown')
                    total_reviews = reviews_data.get('total_reviews', 0)

                    for review in reviews_data.get('reviews', []):
                        review_entry = {
                            'business_id': business_id,
                            'business_name': business_name,
                            'review_id': review.get('id', 'Unknown'),
                            'review_date': review.get('time_created', 'Unknown'), #change to date object
                            'review_text': review.get('text', ''),
                            'review_url': review.get('url', ''),
                            'rating': review.get('rating', 0),
                            'total_reviews': total_reviews,
                            'platform_id': '001' #yelp is 001, google 002  # Manually populate this field as needed
                        }
                        reviews_list.append(review_entry)

                    total_fetched += len(reviews_data.get('reviews', []))
                    if len(reviews_data.get('reviews', [])) < page_size:
                        break
                    page += 1
                    break
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                    return pd.DataFrame()  # Return an empty DataFrame
                except KeyError as e:
                    print(f"KeyError: {e}. Response JSON structure might have changed.")
                    return pd.DataFrame()  # Return an empty DataFrame
            elif response.status_code in [502, 503, 504]:  # Handle server errors
                print(f"Server error {response.status_code}. Retrying... ({attempt + 1}/{retries})")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Error fetching reviews: {response.status_code} - {response.reason}")
                return pd.DataFrame()  # Return an empty DataFrame

    # Convert the list of reviews to a DataFrame
    reviews_df = pd.DataFrame(reviews_list)
    
    return reviews_df


# Example usage
if __name__ == "__main__":
    # Replace with the business ID you want to scrape reviews for
    BUSINESS_ID = 'pearls-deluxe-burgers-san-francisco-3'
    # Number of reviews to fetch
    N_REVIEWS = 50
    
    # Get the reviews
    reviews_df = get_business_reviews(openAI_API_KEY, BUSINESS_ID, N_REVIEWS) ##   DO IT FOR ALL KEYS   
    print(reviews_df)
    
    # Store the reviews in MongoDB
    store_to_mongodb(reviews_df, "yelp_reviews_db", "reviews")

def single_business_scrape_reviews_function(request_data):
    return get_business_reviews(request_data.get("query", 
                                                 None), request_data.get("pages", None), request_data.get("business_count", None))
