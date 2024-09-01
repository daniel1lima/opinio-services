from modules.logger_setup import setup_logger
import json
from typing import List, Tuple
from pydantic import BaseModel, field_validator
import warnings
from sklearn.feature_extraction.text import TfidfVectorizer

# Set up logger specifically for embeddings
logger = setup_logger(log_dir="logs/create_embeddings")

import nltk
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from hdbscan import HDBSCAN
from gensim import corpora, models
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import Counter, defaultdict
from textblob import TextBlob

import os

# Download NLTK resources if needed
nltk.download("punkt")
nltk.download("stopwords")

# Additional stopwords for category refinement
additional_stopwords = {
    "get",
    "great",
    "like",
    "really",
    "good",
    "gym",
    "place",
    "love",
    "hate",
    "one",
    "trainer",
}


class ReviewInput(BaseModel):
    reviews: List[str]

    @field_validator("reviews")
    def check_non_empty(cls, v):
        if not v:
            raise ValueError("Review cannot be empty")
        return v


def _preprocess_text(texts):
    preprocessed_texts = [
        [
            word
            for word in word_tokenize(document.lower())
            if word.isalpha()
            and word not in stopwords.words("english")
            and word not in additional_stopwords
        ]
        for document in texts
    ]
    return preprocessed_texts


def _get_tfidf_embeddings(sentences, vectorizer=None):
    if vectorizer is None:
        vectorizer = TfidfVectorizer(max_df=0.85, min_df=2, ngram_range=(1, 2))
        embeddings = vectorizer.fit_transform(sentences).toarray()
    else:
        embeddings = vectorizer.transform(sentences).toarray()
    return embeddings, vectorizer


def _calculate_center(df):
    centers = (
        df.groupby("Cluster")["tfidf_embeddings"]
        .apply(lambda x: np.mean(np.vstack(x), axis=0))
        .to_dict()
    )
    return centers


def _find_closest_sentence(df, centers):
    closest_sentences = {}
    for cluster, center in centers.items():
        cluster_embeddings = np.vstack(
            df[df["Cluster"] == cluster]["tfidf_embeddings"].values
        )
        distances = np.linalg.norm(cluster_embeddings - center, axis=1)
        closest_index = np.argmin(distances)
        closest_sentences[cluster] = df[df["Cluster"] == cluster]["Sentences"].values[
            closest_index
        ]
    return closest_sentences


def _get_combined_categories(ldamodel, num_topics, num_keywords=5):
    all_keywords = []
    for i in range(num_topics):
        topic_terms = ldamodel.show_topic(i)
        all_keywords.extend([word for word, _ in topic_terms])
    keyword_counts = Counter(all_keywords)
    filtered_keywords = {
        word: count
        for word, count in keyword_counts.items()
        if word not in additional_stopwords
    }
    most_common_keywords = [
        word for word, count in Counter(filtered_keywords).most_common(num_keywords)
    ]
    return most_common_keywords


def analyze_reviews(reviews: List[str]) -> Tuple[dict, dict]:
    """
    Analyze a list of reviews to extract topics, sentiments, and polarities.

    Args:
        reviews (List[str]): A list of review strings.

    Returns:
        Tuple[dict, dict]: A tuple containing two JSON objects:
            - The first JSON object contains the processed reviews with assigned labels and sentiments.
            - The second JSON object contains summaries of average sentiments and polarities for each category.
    """
    # Validate input
    ReviewInput(reviews=reviews)

    logger.info("Starting review analysis")
    preprocessed_reviews = _preprocess_text(reviews)
    dictionary = corpora.Dictionary(preprocessed_reviews)
    corpus = [dictionary.doc2bow(text) for text in preprocessed_reviews]
    num_topics = 10  # Increase the number of topics
    lda_model = models.LdaModel(
        corpus=corpus,
        id2word=dictionary,
        num_topics=num_topics,
        random_state=42,
        passes=20,  # Increase passes
        iterations=150,  # Increase iterations
    )
    labels = _get_combined_categories(lda_model, num_topics)
    processed_reviews = [" ".join(text) for text in preprocessed_reviews]
    df = pd.DataFrame(reviews, columns=["Sentences"])
    df["processed_sentences"] = processed_reviews
    label_texts = [" ".join([label, "review is"]) for label in labels]

    # Use the same TF-IDF vectorizer for both reviews and label texts
    embeddings, vectorizer = _get_tfidf_embeddings(processed_reviews)
    label_embeddings, _ = _get_tfidf_embeddings(label_texts, vectorizer)

    df["tfidf_embeddings"] = list(embeddings)  # Ensure embeddings are added as a list
    clusterer = HDBSCAN(
        min_cluster_size=2,
        min_samples=2,
        metric="euclidean",
        cluster_selection_method="leaf",
        prediction_data=True,
    )
    clusterer.fit(embeddings)
    df["Cluster"] = clusterer.labels_
    centers = _calculate_center(df)
    threshold = 0.5  # Lower the threshold to allow more categories
    assigned_labels = {}
    for cluster_id, centroid in centers.items():
        similarities = cosine_similarity([centroid], label_embeddings)[0]
        assigned_labels[cluster_id] = [
            i for i, sim in enumerate(similarities) if sim > threshold
        ]
        if not assigned_labels[cluster_id]:
            assigned_labels[cluster_id] = [np.argmax(similarities)]

    df["assigned_label"] = df["Cluster"].map(assigned_labels)

    # Create a dictionary to map indices to labels
    label_dict = {i: label for i, label in enumerate(labels)}

    # Apply the mapping with a check for iterable values
    df["named_labels"] = df["assigned_label"].apply(
        lambda x: [label_dict[num] for num in x] if isinstance(x, list) else []
    )

    sentiments = [TextBlob(review).sentiment[0] * 2.5 + 2.5 for review in reviews]
    polarities = [TextBlob(review).sentiment[1] * 2.5 + 2.5 for review in reviews]
    df["sentiment"] = sentiments
    df["polarity"] = polarities

    # Convert all int64 and float64 to standard Python types
    for col in ["sentiment", "polarity"]:
        df[col] = (
            df[col]
            .astype(float)
            .apply(lambda x: x.item() if isinstance(x, (np.int64, np.float64)) else x)
        )

    df.drop(
        columns=["tfidf_embeddings", "processed_sentences", "Sentences", "Cluster"],
        inplace=True,
    )

    categories_summaries_sentiments = defaultdict(list)
    categories_summaries_polarities = defaultdict(list)
    for i, row in df.iterrows():
        for label in row["named_labels"]:
            categories_summaries_sentiments[label].append(row["sentiment"])
            categories_summaries_polarities[label].append(row["polarity"])
    df_summaries = pd.DataFrame(
        {
            "Category": categories_summaries_sentiments.keys(),
            "Average Sentiment": [
                np.mean(vals) for vals in categories_summaries_sentiments.values()
            ],
            "Average Polarity": [
                np.mean(vals) for vals in categories_summaries_polarities.values()
            ],
        }
    )

    # Convert all int64 and float64 in summaries DataFrame
    for col in ["Average Sentiment", "Average Polarity"]:
        df_summaries[col] = (
            df_summaries[col]
            .astype(float)
            .apply(lambda x: x.item() if isinstance(x, (np.int64, np.float64)) else x)
        )

    # Convert DataFrames to JSON
    df_json = df.to_json(
        orient="records"
    )  # Convert processed reviews DataFrame to JSON
    df_summaries_json = df_summaries.to_json(
        orient="records"
    )  # Convert summaries DataFrame to JSON

    logger.info("Review analysis completed")
    return json.loads(df_json), json.loads(df_summaries_json)  # Return JSON objects


if __name__ == "__main__":
    # Example usage
    reviews = [
        "The staff at this gym are incredibly friendly and helpful. They always go the extra mile to make sure I have a great workout experience.",
        "The equipment is top-notch and well-maintained. They have a wide variety of machines for all my training needs.",
        "The gym is always clean and well-organized. It's a pleasure to work out in such a pleasant environment.",
        "The equipment sucks!",
        "The staff is amazing!",
    ]
    df, df_summaries = analyze_reviews(reviews)

    df.to_csv("df.csv")

    print(df_summaries)
    print(df)
