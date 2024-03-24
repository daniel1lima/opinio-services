name = "Golds UBC"
rating = 4.3
count = 436
reviews = [
    ("equipment", 0.8),
    ("cleanliness", -0.5),
    ("pricing", 0.2),
    ("staff", 0.9),
    ("accessibility", 0.1)
]

def calculate_fit_score(gym_name, average_rating, review_count, individual_reviews):
 
    average_review_count = 600
    category_weight = 12  
    
    # 1. Calculate Star Rating Contribution (30% of total score)
    star_rating_contribution = (average_rating / 5) * 30
    
    # 2. Calculate Review Count Rating Contribution (10% of total score)
    if review_count >= average_review_count:
        review_count_rating = 5
    else:
        review_count_rating = 1 + ((review_count / average_review_count) * 4)
    review_count_contribution = (review_count_rating / 5) * 10
    
    # 3. Process individual reviews for category scores
    category_scores = {category: [] for category, _ in individual_reviews}
    for category, score in individual_reviews:
        category_scores[category].append(score)

    # Calculate the sum of scores for each category, transform to 0-5 scale, and find contribution
    category_contributions = {}
    for category, scores in category_scores.items():
        if scores:  # Check if there are scores to avoid division by zero
            average_score = sum(scores) / len(scores)
            category_score = ((average_score + 1) * 2.5)  # Scale from -1-1 to 0-5
            category_contributions[category] = (category_score / 5) * category_weight
    
    # 4. Combine all contributions for final FitScore
    final_fitscore = star_rating_contribution + review_count_contribution + sum(category_contributions.values())
    

    return {
        "gym_name": gym_name,
        "FitScore": final_fitscore, 
    }

final = calculate_fit_score(name, rating, count, reviews)
print(final)
