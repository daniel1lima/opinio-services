import requests
import json
from pymongo import MongoClient
import pandas as pd
import os
import json
import pprint

# First we want to grab all relevant businesses that match the query request

def yelp_script(query, page_count, business_count):
    url = "https://yelp-reviews.p.rapidapi.com/business-search"

    business_query = query

    business_final = pd.DataFrame(columns=["status", "request_id", "data"])
    for i in range(1, business_count):
        print("Page: " + str(i))
        querystring = {"query": business_query, "location": "Vancouver, BC, Canada", "start": str(i * 10), "yelp_domain": "yelp.com"}

        headers = {
            "X-RapidAPI-Key": "52f0c4fb7cmsh68305c1877afa13p1710b0jsn6ea7e5079542",
            "X-RapidAPI-Host": "yelp-reviews.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)

        print(response.json())


        # Create the DATA folder if it doesn't exist
        if not os.path.exists("DATA"):
            os.makedirs("DATA")

        if response.json()['data'] == []:
            print("breaking")
            break

        # Convert the JSON data to a pandas DataFrame
        df = pd.DataFrame(response.json())

        # Concatenate the DataFrame to business_final
        business_final = pd.concat([business_final, df], ignore_index=True)

        # Pretty print the JSON data
        pprint.pprint(business_final)

    # Save the Results_final DataFrame to a CSV file
    business_final.to_csv("DATA/businesses_for_" + business_query + ".csv")


    Businesses = business_final
    BusinessIds = pd.DataFrame(columns = ["businessId", "link", "name", "rating", "review_count"])

    for business in Businesses['data']:
        try:
            business_id = business['id']
        except KeyError:
            business_id = 0
        
        try:
            business_link = business['business_page_link']
        except KeyError:
            business_link = 0
        
        try:
            business_name = business['name']
        except KeyError:
            business_name = 0
        
        try:
            business_rating = business['rating']
        except KeyError:
            business_rating = 0
        
        try:
            business_review_count = business['review_count']
        except KeyError:
            business_review_count = 0
        
        BusinessIds.loc[len(BusinessIds)] = [business_id, business_link, business_name, business_rating, business_review_count]

    BusinessIds



    Results_final = pd.DataFrame(columns = ["name", "overall_rating", "review_text", "date", "review_count"])

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
        
        if reviews_data['status'] == 'ERROR':
            print("Error occurred while fetching reviews for business:", business_name)
            print("Error message:", reviews_data)
            Results_final.to_csv("DATA/reviews_for_" + query + ".csv")
            break

        print(reviews_data)

        reviews_data = reviews_data['data']['reviews']


        try:
            for review in reviews_data:
                try:
                    text = review['review_text']
                    date = review['review_datetime_utc']
                    Results_final.loc[len(Results_final)] = [business_name, business_rating, text, date, business_review_count]
                except TypeError as e:
                    print("Error occurred while processing review:", e)
                    break
        except:
            print("Error occurred while fetching reviews for business:", business_name)
            print("Error message:", reviews_data)
            continue


    Results_final


    Results_final.to_csv("DATA/LONG_reviews_for_" + business_query + ".csv")


yelp_script("nightclubs", 10, 7)