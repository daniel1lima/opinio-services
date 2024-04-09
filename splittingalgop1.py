import pandas as pd
from collections import Counter
import re
from nltk.corpus import stopwords

def load_and_count_keywords_from_csv(csv_file_path):
    # Load the stop words
    stop_words = set(stopwords.words('english'))
    
    # Load the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Access the reviews from the 'review_text' column
    reviews = df['review_text'].tolist()
    
    # Tokenize and count words, excluding stop words
    word_counts = Counter()
    
    for review in reviews:
        # Use regex to find words, this will help in cleaning the text a bit
        words = re.findall(r'\b\w+\b', review.lower())
        # Filter out the stop words
        words = [word for word in words if word not in stop_words]
        word_counts.update(words)
    
    
    word_counts = word_counts.most_common()
    
    return word_counts


csv_file_path = '/Users/yasharya/FitSight-Produhacks2024/DATA/sentiment_reviews_withcount.csv'


word_counts = load_and_count_keywords_from_csv(csv_file_path)
for word, count in word_counts[:12]:  # Just print the top 10 for brevity
    print(f'{word}: {count}')
