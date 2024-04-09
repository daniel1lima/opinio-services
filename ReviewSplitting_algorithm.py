import pandas as pd
from collections import Counter
import re
from nltk.corpus import stopwords

def load_and_count_keywords_from_csv(csv_file_path, additional_stopwords=[]):
    stop_words = set(stopwords.words('english'))
    stop_words.update(additional_stopwords)
    df = pd.read_csv(csv_file_path)
    reviews = df['review_text'].tolist()
    
   
    word_counts = Counter()
    for review in reviews:
        words = re.findall(r'\b\w+\b', review.lower())
        words = [word for word in words if word not in stop_words]
        word_counts.update(words)
    

    top_keywords = [word for word, count in wo
    rd_counts.most_common(5)]
    categorized_reviews = {keyword: [] for keyword in top_keywords}
    

    for review in reviews:
        review_lower = review.lower()
        for keyword in top_keywords:
            if keyword in review_lower:
                categorized_reviews[keyword].append(review)
    
    return categorized_reviews, top_keywords

csv_file_path = '/Users/yasharya/FitSight-Produhacks2024/DATA/sentiment_reviews_withcount.csv'


additional_stopwords = ['gym', 'get', 'one', 'great', 'really', 'good', 'like', 'place']  # Example additional words to exclude


categorized_reviews, top_keywords = load_and_count_keywords_from_csv(csv_file_path, additional_stopwords)


for keyword in top_keywords:
    print(f"Reviews categorized under the keyword '{keyword}':")
    for review in categorized_reviews[keyword][:20]:  
        print(f"- {review}")
    print("\n \n \n") 
