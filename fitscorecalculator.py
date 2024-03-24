import pandas as pd

filepath = '/Users/yasharya/FitSight-Produhacks2024/DATA/sentiment_reviews_withcount.csv'

df = pd.read_csv(filepath)

if 'FitScore_x' in df.columns:
    df = df.drop(columns=['FitScore_x'])

if 'FitScore_y' in df.columns:
    df = df.drop(columns=['FitScore_y'])
    

def calculate_fit_score(df):
    # Define weights for the components
    star_rating_weight = 0.35
    review_count_weight = 0.05
    category_weight = 0.12  

    # Calculate average review count rating
    avg_review_threshold = 600
    df['review_count_rating'] = df['review_count'].apply(lambda x: 5 if x >= avg_review_threshold else (x / avg_review_threshold) * 4 + 1)

    # Aggregate the gym data
    gym_aggregated = df.groupby('name').agg({
        'equipment': 'mean',
        'cleanliness': 'mean',
        'pricing': 'mean',
        'accessibility': 'mean',
        'staff': 'mean',
        'review_count_rating': 'mean',
        'overall_rating': 'mean',
        'review_count': 'mean'  # Just to keep the review count info
    }).reset_index()

    # Adjust category scores from -1 to 1 scale to 0 to 5 scale
    categories = ['equipment', 'cleanliness', 'pricing', 'accessibility', 'staff']
    for category in categories:
        gym_aggregated[category] = gym_aggregated[category] * 2.5 + 2.5

    # Calculate contributions to FitScore
    gym_aggregated['star_rating_contribution'] = gym_aggregated['overall_rating'] * star_rating_weight * 20  # Scale overall_rating (1-5) to fit in 0-100 scale
    gym_aggregated['review_count_contribution'] = gym_aggregated['review_count_rating'] * review_count_weight * 20  # Scale review count rating (1-5) to fit in 0-100 scale

    # Calculate category contributions
    for category in categories:
        gym_aggregated[f'{category}_contribution'] = gym_aggregated[category] * category_weight * 20  # Scale category score (0-5) to fit in 0-100 scale

    # Final FitScore Calculation
    gym_aggregated['FitScore'] = gym_aggregated['star_rating_contribution'] 
    for category in categories:
        gym_aggregated['FitScore'] += gym_aggregated[f'{category}_contribution']

    return gym_aggregated[['name', 'FitScore']]



fit_scores_df = calculate_fit_score(df)
df = df.merge(fit_scores_df, on='name', how='left')
df_updated = pd.read_csv(filepath)

if 'FitScore_x' in df_updated.columns:
    df_updated = df_updated.drop(columns=['FitScore_x'])

if 'review_count_rating' in df_updated.columns:
    df_updated = df_updated.drop(columns=['review_count_rating'])

             
df_updated.to_csv(filepath, index=False)
print("Updated CSV with FitScores.")


