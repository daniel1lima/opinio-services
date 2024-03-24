import requests
import json
from pymongo import MongoClient
import pandas as pd

# First we want to grab all relevant businesses that match the query request

def yelp_script(query, page_count):
    url = "https://yelp-reviews.p.rapidapi.com/business-search"

    business_query = query


    querystring = {"query":business_query,"location":"Vancouver, BC, Canada","start":"0","yelp_domain":"yelp.com"}

    headers = {
	"X-RapidAPI-Key": "52f0c4fb7cmsh68305c1877afa13p1710b0jsn6ea7e5079542",
	"X-RapidAPI-Host": "yelp-reviews.p.rapidapi.com"
}

    response = requests.get(url, headers=headers, params=querystring)

    print(response.json())




    import os
    import json
    import pprint
# Create the DATA folder if it doesn't exist
    if not os.path.exists("DATA"):
        os.makedirs("DATA")

# Specify the file path
    file_path = "DATA/business_data.json"

# Save the dictionary to the JSON file
    with open(file_path, 'w') as f:
        json.dump(response.json(), f)

# Load the JSON data from the file
    with open(file_path, 'r') as f:
        data = json.load(f)

# Pretty print the JSON data
    pprint.pprint(data)


    Businesses = data
    BusinessIds = pd.DataFrame(columns = ["businessId", "link", "name", "rating", "review_count"])

    for business in Businesses['data']:
        BusinessIds.loc[len(BusinessIds)] = [business['id'], business['business_page_link'], business['name'], business['rating'], business['review_count']]

    BusinessIds



    Results_final = pd.DataFrame(columns = ["gym_name", "overall_rating", "review_text", "date", "review_count"])

    pages = str(page_count)


    for index, row in BusinessIds.iterrows():
        url = "https://yelp-reviews.p.rapidapi.com/business-reviews"

    
        business_id = row['businessId']
        business_name = row['name']
        business_rating = row['rating']
        business_review_count = row['review_count']
    

        BUSINESS_ID = business_id
    

        querystring = {"business_id":BUSINESS_ID,"page":"1","page_size":"10","num_pages":pages,"language":"en"}

        headers = {
        "X-RapidAPI-Key": "52f0c4fb7cmsh68305c1877afa13p1710b0jsn6ea7e5079542",
        "X-RapidAPI-Host": "yelp-reviews.p.rapidapi.com"
    }

        response = requests.get(url, headers=headers, params=querystring)

        reviews_data = response.json()

        reviews_data = reviews_data['data']['reviews']

        for review in reviews_data:
            text = review['review_text']
            date = review['review_datetime_utc']

            Results_final.loc[len(Results_final)] = [business_name, business_rating, text, date, business_review_count]


    Results_final


    Results_final.to_csv("DATA/final_results.csv")