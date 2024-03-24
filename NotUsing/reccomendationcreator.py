import csv
from statistics import mean
import pandas as pd
from openai import OpenAI

# Configuration
csv_file_path = '/Users/yelhagra/Desktop/FitSight-Produhacks2024/sentiment_reviews.csv'  # Update this with your actual CSV file path
client = OpenAI(api_key = "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")  # Replace this with your actual OpenAI API key

def read_and_process_reviews(csv_path):
    reviews_with_averages = []

    dtype_mapping = {
        'name': str,
        'equipment': float,
        'cleanliness': float,
        'pricing': float,
        'accessibility': float,
        'staff': float,
        'review_text': str
    }

    df = pd.read_csv(csv_path, index_col="Unnamed: 0", header=0, dtype=dtype_mapping)

    columns_to_sum = ['equipment', 'cleanliness', 'pricing', 'accessibility', 'staff']
    df['sum_rating'] = df[columns_to_sum].sum(axis=1)
    df_selected = df[columns_to_sum]
    none_zero_rows = (df_selected != 0).sum(axis = 1)
    df['Non_zero_count'] = none_zero_rows
    df['average_rating'] = df['sum_rating'] / df['Non_zero_count']
    df = df.sort_values(by='average_rating', ascending=True)
    #only pick 4 worst reviews from each gym
    df_worst_reviews = df.groupby('name').head(3)

    reviews_dict = dict()
    for name in df_worst_reviews['name']:
        row = df_worst_reviews[df_worst_reviews['name'] == name]
        reviews = row['review_text'].tolist()
        #convert to string
        reviews2 = ""
        for i in reviews:
            reviews2 += " " + i

        reviews_dict[name] = reviews2

    return reviews_dict

def generate_action_items(review_text):
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "assistant",
                 "content": "Based on the review provided, give four action items that the gym can take to improve the customer experience. Be specific and include numbers where possibly. Action items need to be timely."},
                {"role": "user", "content": review_text}
            ]
        )
        print(response.choices[0].message.content)
        return response.choices[0].message.content
    except Exception as e:
        print(f"An error occurred: {e}")
        return ""
#change csv_file_path to mongoDb csv later
def main():
    sorted_reviews = read_and_process_reviews(csv_file_path)
    final_df = pd.DataFrame(columns = ["gym_name", "action_items"])
    for gym_name, review_text in sorted_reviews.items():
        action_items = generate_action_items(review_text)
        #remove "\n" from action_items
        action_items = action_items.replace("\n", "")
        final_df = final_df.append({"gym_name": gym_name, "action_items": action_items}, ignore_index=True)
    final_df.to_csv('DATA/action_items.csv')



if __name__ == '__main__':
    main()
