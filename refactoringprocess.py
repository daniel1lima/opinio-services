import pandas as pd
from gensim import corpora, models
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem import WordNetLemmatizer
import re

def analyze_reviews_from_csv(csv_path, review_column='review_text'):

    

    
    df = pd.read_csv(csv_path)
    reviews = df[review_column].tolist()

    
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()

    def preprocess_text(texts):
        return [
            [lemmatizer.lemmatize(word) for word in word_tokenize(document.lower()) if word.isalpha() and word not in stop_words]
            for document in texts
        ]

    preprocessed_reviews = preprocess_text(reviews)

    
    dictionary = corpora.Dictionary(preprocessed_reviews)
    corpus = [dictionary.doc2bow(text) for text in preprocessed_reviews]
    num_topics = 3
    lda_model = models.LdaModel(corpus=corpus, id2word=dictionary, num_topics=num_topics, random_state=42, passes=15, iterations=100)

    # Manual Mapping of Keywords to Categories
    keyword_to_category = {
        'bench': 'Equipment',
        'food': 'Food',
        'pasta': 'Food',
        'reservation': 'Reservation',
        'parking': 'Parking',
        'server': 'Staff',
        'staff': 'Staff'
    }

    def get_category_name(lda_topic_keywords, mapping):
        for keyword in lda_topic_keywords:
            if keyword in mapping:
                return mapping[keyword]
        return "Other"  # Fallback category

    topic_keywords_improved = {}
    for i in range(num_topics):
        lda_keywords = [word for word, _ in lda_model.show_topic(i, 5)]  # Extracting top 5 keywords
        topic_keywords_improved[i] = get_category_name(lda_keywords, keyword_to_category)

    # Categorizing Phrases with Improved Topic Names
    def categorize_phrases(ldamodel, corpus, texts, topic_keywords):
        category_phrases = {topic_keywords[i]: [] for i in range(num_topics)}
        for review in texts:
            bow = dictionary.doc2bow(preprocess_text([review])[0])
            topics = ldamodel.get_document_topics(bow)
            topics = sorted(topics, key=lambda x: x[1], reverse=True)
            if topics:
                topic_num, _ = topics[0]
                sentences = sent_tokenize(review)
                for sentence in sentences:
                    clean_sentence = re.sub("[^a-zA-Z ]", "", sentence).lower()
                    category_name = topic_keywords[topic_num]
                    category_phrases[category_name].append(clean_sentence.capitalize())
        return category_phrases

    categorized_phrases = categorize_phrases(lda_model, corpus, reviews, topic_keywords_improved)

    return categorized_phrases



csv_path = '/Users/yasharya/FitSight-Produhacks2024/DATA/sentiment_reviews_withcount.csv'
review_column = 'review_text'

categorized_phrases_output = analyze_reviews_from_csv(csv_path, review_column)


for category, phrases in categorized_phrases_output.items():
    print(f"{category}:")
    for phrase in phrases:
        print(f" - {phrase}")
    print("\n")
