import pandas as pd
import requests
import re
import time

gym_names = ["Gold's Gym University Marketplace", 'Robert Lee YMCA', 'Equinox W Georgia St', 'Anytime Fitness Denman', 'Train On Main',
 'House Concepts',
 'Studeo',
 'Spartacus Gym',
 'GoodLife Fitness Vancouver Hemlock and 8th For Women',
 'Dynasty Gym']

id_list = ['U2FsdGVkX1%2BP3ic%2BO7%2FuG2EZwiyt3xItwV5%2BdxHD5xeQEE3HFu2c2gjg00xvnCxkY4sfzm1X0zfe6O5V%2BHlpfA%3D%3D',
 'U2FsdGVkX1%2FndoEU0Vg%2B2jhJsKlaSIVg64%2Ft9b3gwPoFIIFKRuKk4rSv9nhItoPni9%2FCx8rX3j4aPeThUp6s%2Fg%3D%3D',
 'U2FsdGVkX1%2Bl7MStA07vdrJdpU6UhHm8ssh7P6zknB%2FpRB6ha0I9TXOsF70i2I%2Fnmdnz46YCgJ6i4Eac5DmGDw%3D%3D',
 'U2FsdGVkX19ZFw2yAuZ4knCRSC9MYokuyAgNkFj6qHE%2BVmUIsj%2BPISbvDkBs3olvY8Bj%2FqAnKullSH%2Ftrdo8BA%3D%3D',
 'U2FsdGVkX195frqUlfA07wWFc2CiCGbGYO%2BQwBMkulUiOTvqxkQwdKMv1MYVqHCkcnw%2BSjtwQy9ffk4qm0OFlA%3D%3D',
 'U2FsdGVkX18JgeZiSWuMay4LxhH9LpD5mGdLNtbCo2ptvDswBRY0n54J6e887iBCQ8dcB2qaOH0TSf%2FsvFvx%2Fg%3D%3D',
 'U2FsdGVkX18U03zORxAEyAkAON%2FUSlqCooeLq%2B%2BaDU%2FlyMPsjHbn2AdCPf%2Bzzpcvb%2BT9H%2BsNCWVhDh8hMe%2BhkA%3D%3D',
 'U2FsdGVkX1%2FA2zgNQnWU4ZFLnqI%2Fx6yPv%2BtNYAjeCodJITdtSEjQ6LSXz4GTzcyuslsnpWmAckqsLDZaKq5TVw%3D%3D',
 'U2FsdGVkX187vj48a9IugfiBeoHywuV9%2FB%2BO8TeyeCDVVK2luhHcpHZNZBb9zWO4DsYa%2BfUm%2FeLcCqvQiCUnug%3D%3D',
 'U2FsdGVkX1%2F1PNnEStP%2BN6%2FqKfo2PjFylaJYqMprOMwqT3sGg3xWBQDpSVHNw4VpUQIjwqmu31K3KB%2F4QS9tHg%3D%3D',
 'U2FsdGVkX1%2FmmtRp4hJYaqK7N%2F0xGQUPt2Q3CKV%2FMKUg27pHrDmOdrKvf67fA66NW6SGnpqTMhrH1SuMxw8ZsA%3D%3D',
 'U2FsdGVkX19MkvzL9ivWU9EIgfkuxuUsGSeIN6zDI%2FeW8dOxOa8b50eIneX7%2FG7jmaqp8vBejWjJdwogFuXwcQ%3D%3D',
 'U2FsdGVkX19QPbeM37WnRFbskiDLsREEZUpE%2F8hKq7NCyW36YM3gFZ%2BLXfO0KySZws7nIqm4Dzi69x2mzD%2Bp6w%3D%3D',
 'U2FsdGVkX19movbu0R2SoNyMxfsuXeQRER6gfzY0zoCUD6ik%2Bh4pdt0eVKtWifXUX%2FVs%2B%2FFy7GLMBVY6CzYQog%3D%3D',
 'U2FsdGVkX1%2FSDG8sFN3ehRLZQNfBkT%2B8tJ9Y0lkoxnQM7zOSfIjUeHAMqJlUedopXOhbmqXKKeRpjtd0oqDSJA%3D%3D',
 'U2FsdGVkX18W05yIan6FAh95IzuIclpk43MhMrvsEfu9Gen4zyJxui4k4Pm1W%2BfysIMNp2BmgpqHzA5MxiKxOQ%3D%3D',
 'U2FsdGVkX19%2BMHAM8MhTxTsiH2VZHIfGbDjyWQs0K98Lz6P5DuYctMEOG%2B4pfEsxi8kbRl%2BalsrwE4JygdEXcg%3D%3D',
 'U2FsdGVkX1%2FfkPP2iFs%2BH2p7hMkRt%2BdOv4N1edn9MX%2Fpq61eoHVMoji9iGAOcR2pQQFtFBtgeJd%2FjiLQIPJShQ%3D%3D',
 'U2FsdGVkX19YTq65b8v23KamsXFknbLceu6r6YKJtztOFFF84kgMqLHvLxPEWvzT3LkKeaJVtXf0pxP9fSprog%3D%3D']

id_list = id_list[0:10]

final_df_reviews = pd.DataFrame(columns=["gym_name", "overall_rating", "review_text", "date"])

df_gym_overall = pd.DataFrame(columns=["gym_name", "average_rating", "number_of_reviews"])


def review_adder(gym_name, gym_id):
    global final_df_reviews
    global df_gym_overall

    url = "https://google-reviews-scraper.p.rapidapi.com/"

    querystring = {"fullid": f"{gym_id}", "fullsort": "relevant"}

    headers = {
        "X-RapidAPI-Key": "52f0c4fb7cmsh68305c1877afa13p1710b0jsn6ea7e5079542",
        "X-RapidAPI-Host": "google-reviews-scraper.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    response = response.json()

    print(response)

    for review in response['reviews']:

        input_string = review["rating"]

        # Regular expression pattern to match the rating
        pattern = r"Rated\s(\d+\.\d+)\sout of 5,"

        # Search for the pattern in the input string
        match = re.search(pattern, input_string)

        if "month" not in review["date"]:
            if "months" not in review["date"]:
                if "a year ago" not in review["date"]:
                    continue

        if "comment" not in review.keys():
            continue

        data_to_append = {'gym_name': f"{gym_name}",
                          'overall_rating': match.group(1),
                          'review_text': review["comment"],
                          'date': review["date"]}

        final_df_reviews = final_df_reviews.append(data_to_append, ignore_index=True)

    data_2_append = {'gym_name': f"{gym_name}",
                     "average_rating": response['averageRating'],
                     "number_of_reviews": response['totalReviews']}

    df_gym_overall = df_gym_overall.append(data_2_append, ignore_index=True)


def gym_searcher(gym_name):
    url = "https://google-reviews-scraper.p.rapidapi.com/"

    querystring = {"keyword": f"{gym_name}"}

    headers = {
        "X-RapidAPI-Key": "52f0c4fb7cmsh68305c1877afa13p1710b0jsn6ea7e5079542",
        "X-RapidAPI-Host": "google-reviews-scraper.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    name = response.json()['results'][0]['id']
    print(response.json()['results'][0]['id'])
    return name


for i in range(0, 10):
    curr_name = gym_names[i]
    curr_id = id_list[i]
    review_adder(curr_name, curr_id)
    time.sleep(1)
    print(final_df_reviews)
    print(df_gym_overall)

final_df_reviews.to_csv("gym_reviews.csv")
df_gym_overall.to_csv("gym_overall.csv")

# Path: Google_reviews.py


