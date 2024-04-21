# Import necessary libraries
import gensim
from gensim import corpora, models
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
import re

# Ensure necessary NLTK downloads
nltk.download('stopwords')
nltk.download('punkt')

# Sample reviews
reviews = [
    "Food was great but staff was slightly rude",
    "Loved the pasta and Marie was a great server",
    "Was able to get a reservation easily"
]

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

# Function to get the dominant topic for each sentence
def format_topics_sentences(ldamodel=None, corpus=corpus, texts=reviews):
    # Init output
    sent_topics_df = []
    
    # Get main topic in each document
    for i, row_list in enumerate(ldamodel[corpus]):
        row = row_list[0] if ldamodel.per_word_topics else row_list            
        # print(row)
        row = sorted(row, key=lambda x: (x[1]), reverse=True)
        # Get the Dominant topic, Perc Contribution and Keywords for each document
        for j, (topic_num, prop_topic) in enumerate(row):
            if j == 0:  # => dominant topic
                wp = ldamodel.show_topic(topic_num)
                topic_keywords = ", ".join([word for word, prop in wp])
                sent_topics_df.append((i, round(prop_topic,4), topic_num, topic_keywords))
            else:
                break
    return sent_topics_df

topic_data = format_topics_sentences(ldamodel=lda_model, corpus=corpus, texts=reviews)

# Splitting the reviews into phrases based on the dominant topic of each sentence
def categorize_sentences(reviews, topic_data):
    categorized_sentences = {i: [] for i in range(num_topics)}
    for review, data in zip(reviews, topic_data):
        _, _, topic_num, _ = data
        sentences = sent_tokenize(review)
        for sentence in sentences:
            # Remove non-alphabetical characters for cleaner comparison
            clean_sentence = re.sub("[^a-zA-Z ]", "", sentence).lower()
            categorized_sentences[topic_num].append(clean_sentence.capitalize())
    return categorized_sentences

categorized_sentences = categorize_sentences(reviews, topic_data)

# Displaying categorized sentences
for topic, sentences in categorized_sentences.items():
    print(f"Topic {topic}:")
    for sentence in sentences:
        print(f" - {sentence}")
    print("\n")
