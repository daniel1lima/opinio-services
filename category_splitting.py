import pandas as pd
import gensim
from gensim import corpora, models
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import Counter
import re

# Ensure necessary NLTK downloads
nltk.download('stopwords')
nltk.download('punkt')

# Load reviews from a CSV file
df = pd.read_csv('/Users/yasharya/Projects/Opinio/FitSight-Produhacks2024/DATA/sentiment_reviews_withcount.csv')
reviews = df['review_text'].tolist()  # Replace 'review_text' with your column name if different

# Preprocess reviews: Tokenization, removing stopwords, non-alphabetical characters
def preprocess_text(texts):
    stop_words = set(stopwords.words('english'))
    preprocessed_texts = [[word for word in word_tokenize(document.lower()) if word.isalpha() and word not in stop_words]
             for document in texts]
    return preprocessed_texts

preprocessed_reviews = preprocess_text(reviews)

# Create a dictionary and corpus for LDA
dictionary = corpora.Dictionary(preprocessed_reviews)
corpus = [dictionary.doc2bow(text) for text in preprocessed_reviews]

# Apply LDA
num_topics = 3  # Adjust based on your data and needs
lda_model = models.LdaModel(corpus=corpus, id2word=dictionary, num_topics=num_topics, random_state=42, passes=15, iterations=100)

# Additional stopwords for category refinement
additional_stopwords = {'get', 'great', 'like', 'really', 'good'}  # Add more words as needed

# Function to get combined categories from the LDA model, excluding certain common words
def get_combined_categories(ldamodel, num_topics, num_keywords=5):
    # Collect all words from all topics
    all_keywords = []
    for i in range(num_topics):
        topic_terms = ldamodel.show_topic(i)
        all_keywords.extend([word for word, _ in topic_terms])

    # Count the frequency of each word
    keyword_counts = Counter(all_keywords)
    
    # Filter out additional stopwords
    filtered_keywords = {word: count for word, count in keyword_counts.items() if word not in additional_stopwords}
    
    # Get the most common words across all topics, after filtering
    most_common_keywords = [word for word, count in Counter(filtered_keywords).most_common(num_keywords)]
    return most_common_keywords

categories = get_combined_categories(lda_model, num_topics)

# Displaying combined categories
print("Categories:", ", ".join(categories))
