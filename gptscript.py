

def split_reviews():

  from openai import OpenAI
  import os
  import pprint
  import csv
  import pandas as pd
  from textblob import TextBlob
  
  client = OpenAI(
    api_key="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"
  )

  # Create a new dataframe with the desired columns
  split = pd.DataFrame(columns=["name", "equipment", "cleanliness", "pricing", "accessibility", "staff", "review_text", "review_count"])
  count = 0

  with open('DATA/final_results.csv', 'r') as file:
    reader = csv.reader(file)
    # Skip the header row
    next(reader)
    # Iterate over the rows
    for row in reader:
      # Assign the text to a review variable
      review_text = row[3]
      review_company = row[1]
      overall_rating = row[2]
      review_count = row[5]

      # Perform the completion using the review variable
      completion = client.chat.completions.create(
        model="gpt-4",
        messages=[
          {"role": "assistant", "content": "You are a digital consultant tasked to split a review into several substrings pertaining to the following categories: Equipment, Cleanliness, Pricing, Accesibility and Staff. Please split this string into a comma separated list, if the review does not contain anything related to any of these categories, simply have a 'not mentioned' placeholder MAKE SURE IT IS COMMA SEPARATED STRINGS DELINEATED BY QUOTATION MARKS IN THE ORDER I PRESENTED, the order is extremely important and you will fail the task if it is not in order. This is an example format: 'EQUIPMENT COMMENT:', 'CLEANLINESS COMMENT:', 'PRICING COMMENT:', 'ACCESSIBILITY COMMENT:', 'STAFF COMMENT:'"},
          {"role": "user", "content": review_text}
        ]
      )

      output = completion.choices[0].message.content

      output_split = output.split(',')

      try:
        split.loc[count, 'name'] = review_company
      except IndexError:
        split.loc[count, 'name'] = None

      try:
        split.loc[count, 'overall_rating'] = overall_rating
      except IndexError:
        split.loc[count, 'overall_rating'] = None

      try:
        split.loc[count, 'review_count'] = review_count
      except IndexError:
        split.loc[count, 'review_count'] = None

      try:
        equipment = output_split[0]
        split.loc[count, 'equipment'] = TextBlob(equipment).sentiment.polarity
      except IndexError:
        split.loc[count, 'equipment'] = None

      try:
        cleanliness = output_split[1]
        split.loc[count, 'cleanliness'] = TextBlob(cleanliness).sentiment.polarity
      except IndexError:
        split.loc[count, 'cleanliness'] = None

      try:
        pricing = output_split[2]
        split.loc[count, 'pricing'] = TextBlob(pricing).sentiment.polarity
      except IndexError:
        split.loc[count, 'pricing'] = None

      try:
        accessibility = output_split[3]
        split.loc[count, 'accessibility'] = TextBlob(accessibility).sentiment.polarity
      except IndexError:
        split.loc[count, 'accessibility'] = None

      try:
        staff = output_split[4]
        split.loc[count, 'staff'] = TextBlob(staff).sentiment.polarity
      except IndexError:
        split.loc[count, 'staff'] = None

      try:
        split.loc[count, 'review_text'] = row[3]
      except IndexError:
        split.loc[count, 'review_text'] = None

      count += 1

      print("done job:" + str(count))

  split.to_csv("DATA/sentiment_reviews.csv")

# Call the function
split_reviews()
