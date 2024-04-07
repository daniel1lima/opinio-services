import gensim
from gensim import corpora, models
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from transformers import pipeline

def dynamic_review_categorization(reviews, num_topics=5, categories=None):
    # Ensure nltk resources are downloaded
    nltk.download('stopwords')
    nltk.download('punkt')

    # Preprocess Text: Tokenization and Removing Stop Words
    def preprocess_text(texts):
        stop_words = set(stopwords.words('english'))
        return [
            [word for word in word_tokenize(document.lower()) if word.isalpha() and word not in stop_words]
            for document in texts
        ]

    # LDA Model to Find Topics
    preprocessed_reviews = preprocess_text(reviews)
    dictionary = corpora.Dictionary(preprocessed_reviews)
    corpus = [dictionary.doc2bow(text) for text in preprocessed_reviews]
    lda_model = models.LdaModel(corpus=corpus, id2word=dictionary, num_topics=num_topics, random_state=42, passes=15, iterations=100)

    # Extract Keywords from Topics
    top_keywords_per_topic = {i: [word for word, _ in lda_model.show_topic(i, topn=5)] for i in range(lda_model.num_topics)}

    # Zero-shot Classification to Dynamically Map Topics to Categories
    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")
    if not categories:
        categories = ["food", "service", "ambiance", "location", "price", "equipment", "classes", "staff", "parking", "reservation", "cleanliness", "accessibility"]

    topic_to_category = {}
    for topic_id, keywords in top_keywords_per_topic.items():
        pseudo_sentence = ', '.join(keywords)
        result = classifier(pseudo_sentence, candidate_labels=categories, hypothesis_template="This text is about {}.")
        topic_to_category[topic_id] = result["labels"][0]

    # Categorize Reviews Based on Topics
    def categorize_reviews(lda_model, topic_to_category):
        categorized_reviews = {category: [] for category in set(topic_to_category.values())}
        for review in reviews:
            bow = dictionary.doc2bow(preprocess_text([review])[0])
            topics = lda_model.get_document_topics(bow)
            top_topic = sorted(topics, key=lambda x: x[1], reverse=True)[0][0]
            category = topic_to_category[top_topic]
            categorized_reviews[category].append(review)
        return categorized_reviews

    return categorize_reviews(lda_model, topic_to_category)

# Example usage with your reviews dataset
reviews = [
    "Food was great but staff was slightly rude",
    "Marie was a great server and Loved the pasta",
    "Was able to get a reservation easily",
    "No parking nearby"
]

categorized_reviews = dynamic_review_categorization(reviews)
for category, reviews in categorized_reviews.items():
    print(f"Category: {category}")
    for review in reviews:
        print(f" - {review}")
    print("\n")
