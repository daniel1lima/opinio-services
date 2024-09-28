import json
from bs4 import BeautifulSoup
from selenium import webdriver


def _scrape_reviews_page(offset: int):
    """
    Scrapes a single page of Yelp reviews.

    Args:
        offset (int): The offset to start scraping from.

    Returns:
        List[dict]: A list of raw review data dictionaries.
    """

    links_with_text = []

    business_id = "kirei-cleaning-vancouver-2"

    url = (
        f"https://www.yelp.com/biz/{business_id}?start={offset}&sort_by=date_desc&rr=0"
    )
    print(url)

    # Initialize the WebDriver (assuming Chrome is being used)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--no-sandbox")  # Bypass OS security model
    options.add_argument(
        "--disable-dev-shm-usage"
    )  # Overcome limited resource problems
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        print(soup)
        with open("soup.html", "w") as f:
            f.write(soup.prettify())

        # Find review elements on the page
        review_elements = soup.find_all("li", class_="y-css-mu4kr5")  # Updated selector
        for review in review_elements:
            # Extract author name
            author_name = review.find("a", class_="y-css-12ly5yx")
            author_name = author_name.text.strip() if author_name else "N/A"

            # Extract author location
            author_location = review.find("span", class_="y-css-h9c2fl")
            author_location = author_location.text.strip() if author_location else "N/A"

            # Extract author friends and reviews
            user_passport_stats = review.find(
                "div", class_="user-passport-stats__09f24__NQxB4"
            )
            if user_passport_stats:
                friends_div = user_passport_stats.find("div", {"aria-label": "Friends"})
                author_friends = (
                    int(friends_div.find("span", class_="y-css-evk3sl").text)
                    if friends_div
                    else 0
                )

                reviews_div = user_passport_stats.find("div", {"aria-label": "Reviews"})
                author_reviews = (
                    int(reviews_div.find("span", class_="y-css-evk3sl").text)
                    if reviews_div
                    else 0
                )
            else:
                author_friends = 0
                author_reviews = 0

            # Extract rating
            rating_divs = review.find_all("div", class_="y-css-1tnjuko")
            rating = len(rating_divs) if rating_divs else 0

            # Extract review date
            review_date = review.find("span", class_="y-css-wfbtsu")
            review_date = review_date.text.strip() if review_date else "N/A"

            # Extract review text
            review_text = review.find("p", class_="comment__09f24__D0cxf")
            review_text = review_text.text.strip() if review_text else "N/A"

            # Extract reaction counts
            reactions = review.find_all("div", class_="y-css-csqdt7")
            reaction_counts = [
                0,
                0,
                0,
                0,
            ]  # Default values for helpful, thanks, love, oh no
            for i, reaction in enumerate(reactions[:4]):  # Limit to first 4 reactions
                count_span = reaction.find("span", class_="y-css-cb357x")
                if count_span:
                    reaction_counts[i] = int(count_span.text)

            review_data = {
                "author_name": author_name,
                "author_location": author_location,
                "author_friends": author_friends,
                "author_reviews": author_reviews,
                "rating": rating,
                "review_date": review_date,
                "review_text": review_text,
                "helpful_count": reaction_counts[0],
                "thanks_count": reaction_counts[1],
                "love_count": reaction_counts[2],
                "oh_no_count": reaction_counts[3],
            }
            links_with_text.append(review_data)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        driver.quit()
        with open("reviews.json", "w") as f:
            json.dump(links_with_text, f)

    return links_with_text

    # TODO: Implement web scraping logic here
    # This function should return a list of dictionaries containing raw review data
    pass


if __name__ == "__main__":
    reviews = _scrape_reviews_page(0)
    print(reviews)
