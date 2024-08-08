import pandas as pd
import gensim
from gensim import corpora, models
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import Counter
import re

# Ensure necessary NLTK downloads
nltk.download("stopwords")
nltk.download("punkt")

# Load reviews from a CSV file
# TODO: Make this path dynamic
df = pd.read_csv(
    "/Users/yasharya/Projects/Opinio/FitSight-Produhacks2024/DATA/sentiment_reviews_withcount copy.csv"
)
reviews = df[
    "review_text"
].tolist()  # Replace 'review_text' with your column name if different


# Preprocess reviews: Tokenization, removing stopwords, non-alphabetical characters
def preprocess_text(texts):
    stop_words = set(stopwords.words("english"))
    preprocessed_texts = [
        [
            word
            for word in word_tokenize(document.lower())
            if word.isalpha() and word not in stop_words
        ]
        for document in texts
    ]
    return preprocessed_texts


preprocessed_reviews = preprocess_text(reviews)

# Create a dictionary and corpus for LDA
dictionary = corpora.Dictionary(preprocessed_reviews)
corpus = [dictionary.doc2bow(text) for text in preprocessed_reviews]

# Apply LDA
num_topics = 3  # Adjust based on your data and needs
lda_model = models.LdaModel(
    corpus=corpus,
    id2word=dictionary,
    num_topics=num_topics,
    random_state=42,
    passes=15,
    iterations=100,
)

# Priority keywords and additional stopwords for refinement
priority_keywords = ["cost ", "staff"]
additional_stopwords = {
    "get",
    "great",
    "like",
    "really",
    "good",
}  # Add more words as needed


# Function to get combined categories from the LDA model, including priority keywords
def get_combined_categories(ldamodel, num_topics, num_keywords=5):
    # Collect all words from all topics
    all_keywords = []
    for i in range(num_topics):
        topic_terms = ldamodel.show_topic(i)
        all_keywords.extend([word for word, _ in topic_terms])

    # Count the frequency of each word
    keyword_counts = Counter(all_keywords)

    # Filter out additional stopwords
    filtered_keywords = {
        word: count
        for word, count in keyword_counts.items()
        if word not in additional_stopwords
    }

    # Prioritize specific keywords if they appear
    prioritized = [word for word in priority_keywords if word in filtered_keywords]

    # Fill the remaining keywords from the most common ones, excluding prioritized words
    most_common_keywords = prioritized + [
        word
        for word, count in Counter(filtered_keywords).most_common(num_keywords)
        if word not in prioritized
    ]

    return most_common_keywords[
        :num_keywords
    ]  # Limit to the requested number of keywords


categories = get_combined_categories(lda_model, num_topics)

# Displaying combined categories
print("Categories:", ", ".join(categories))
