import csv
from statistics import mean
import openai  # Make sure to install the openai library

# Replace 'your_api_key_here' with your actual OpenAI API key
openai.api_key = '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'

def read_reviews_and_calculate_averages(csv_path):
    reviews_with_averages = []
    with open(csv_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Extract ratings and calculate the average
            ratings = [float(row[f'Rating{i}']) for i in range(1, 6)]
            average_rating = mean(ratings)
            reviews_with_averages.append((row['GymName'], row['Review'], average_rating))
    # Sort by average rating and select the lowest 100
    sorted_reviews = sorted(reviews_with_averages, key=lambda x: x[2])[:100]
    return sorted_reviews

def generate_action_items(review_text):
    # Simulated API call - Replace this with actual GPT API call
    try:
        response = openai.Completion.create(
            engine="text-davinci-003",
            prompt=f"Given this gym review: '{review_text}', generate 4 specific action items.",
            temperature=0.5,
            max_tokens=100,
            top_p=1.0,
            frequency_penalty=0.0,
            presence_penalty=0.0
        )
        return response.choices[0].text.strip()
    except Exception as e:
        print(f"An error occurred while generating action items: {e}")
        return "N/A"

def main():
    csv_path = '/mnt/data/sentiment_reviews.csv'  # Adjust this path as necessary
    sorted_reviews = read_reviews_and_calculate_averages(csv_path)
    for gym_name, review_text, average_rating in sorted_reviews:
        action_items = generate_action_items(review_text)
        print(f"Gym Name: {gym_name}\nReview: {review_text}\nAverage Rating: {average_rating}\nAction Items: {action_items}\n{'='*60}")

if __name__ == '__main__':
    main()
