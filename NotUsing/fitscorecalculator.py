import pandas as pd

# USE LIKE THIS
# update_fit_scores_in_csv('DATA/sentiment_reviews_withcount.csv')

def update_fit_scores_in_csv(filepath):
    # Read the original CSV file
    df_original = pd.read_csv(filepath)
    
    # Drop the 'FitScore_x', 'FitScore_y', and 'review_count_rating' columns if they exist
    for column in ['FitScore_x', 'FitScore_y', 'review_count_rating']:
        if column in df_original.columns:
            df_original.drop(columns=[column], inplace=True)

    # Calculate average review count rating
    avg_review_threshold = 600
    df_original['review_count_rating'] = df_original['review_count'].apply(
        lambda x: 5 if x >= avg_review_threshold else (x / avg_review_threshold) * 4 + 1)

    # Aggregate the data
    categories = ['equipment', 'cleanliness', 'pricing', 'accessibility', 'staff']
    aggregated = df_original.groupby('name', as_index=False).agg({
        'overall_rating': 'mean',
        'review_count_rating': 'mean',
        **{category: 'mean' for category in categories},
        'review_count': 'mean'
    })

    # Adjust category scores and calculate contributions
    for category in categories:
        aggregated[category] = aggregated[category] * 2.5 + 2.5  # Scale adjustment

    # Define weights for the components
    star_rating_weight = 0.35
    review_count_weight = 0.05
    category_weight = 0.12  

    # Calculate contributions to FitScore
    aggregated['star_rating_contribution'] = aggregated['overall_rating'] * star_rating_weight * 20
    aggregated['review_count_contribution'] = aggregated['review_count_rating'] * review_count_weight * 20
    for category in categories:
        aggregated[f'{category}_contribution'] = aggregated[category] * category_weight * 20

    # Final FitScore Calculation
    aggregated['FitScore'] = aggregated['star_rating_contribution']
    for category in categories:
        aggregated['FitScore'] += aggregated[f'{category}_contribution']

    # Prepare DataFrame for merging
    fit_scores_df = aggregated[['name', 'fitscore']]
    
    # Merge the FitScore back to the original DataFrame
    df_merged = pd.merge(df_original.drop(columns=['review_count_rating']), fit_scores_df, on='name', how='left')

    # Save the updated DataFrame back to the CSV
    df_merged.to_csv(filepath, index=False)
    print("Updated CSV with FitScores.")