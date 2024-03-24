#from textblob import TextBlob
from nltk.sentiment import SentimentIntensityAnalyzer

sia = SentimentIntensityAnalyzer()


categories = {
    "Cleanliness": ["clean", "sanitary", "tidy", "dusty", "dirty"],
    "Equipment": ["machines", "weights", "treadmill", "equipment", "broken"],
    "Staff": ["friendly", "rude", "helpful", "unhelpful", "staff"],
    "Pricing": ["expensive", "cheap", "affordable", "cost", "price"],
    "Accessibility": ["accessible", "parking", "location", "far", "near"]
}



def analyze_sentiment(phrase):
    """Analyze the sentiment of the phrase"""
    analysis = TextBlob(phrase)
    return analysis.sentiment.polarity 

def main():
    
    review_phrases = [
        "The gym is always clean and well maintained",
        "The equipment is often broken and unavailable",
        "Karen the instructor was so rude",
        "OMG my instructor lima was amazing",
        "It's very accessible, with plenty of parking space"
    ]

    for phrase in review_phrases:
        sentiment_score = sia.polarity_scores(phrase)
        
        print(f"Phrase: '{phrase}'\n Sentiment Score: {sentiment_score}\n")


if __name__ == "__main__":
    main()