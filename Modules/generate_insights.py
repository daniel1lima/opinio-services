import os
import requests
import json
from dotenv import load_dotenv
from models.models import CompanyModel, ReviewModel

load_dotenv(override=True)


def generate_insights_using_azure_openai(prompt):
    API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
    ENDPOINT = "https://opinio.openai.azure.com/openai/deployments/4o-mini/chat/completions?api-version=2024-02-15-preview"

    headers = {
        "Content-Type": "application/json",
        "api-key": API_KEY,
    }

    # Updated example JSON to include urgency level
    example_json = """
    {
        "highlights": [
            {
                "title": "Covid Protocol",
                "description": "Your Covid protocol is being well received",
                "percentage": "90%"
            },
            {
                "title": "Clean Bathrooms",
                "description": "People are enjoying how clean your bathrooms are",
                "percentage": "80%"
            },
            {
                "title": "Friendly Staff",
                "description": "Customers appreciate the warm and welcoming staff",
                "percentage": "85%"
            }
        ],
        "lowlights": [
            {
                "title": "Service Speed",
                "description": "Customers report long wait times for their orders",
                "percentage": "40%"
            },
            {
                "title": "Food Quality",
                "description": "Some customers find the food bland and lacking flavor",
                "percentage": "30%"
            },
            {
                "title": "Parking Availability",
                "description": "Limited parking spaces are causing inconvenience",
                "percentage": "35%"
            }
        ],
        "insights": [
            {
                "title": "Menu Innovation and Flavor Enhancement",
                "problem": "Detailed analysis of customer feedback over the past three months reveals a consistent and growing dissatisfaction with the current menu offerings. Specifically, 68% of negative reviews mention bland flavors, while 42% express disappointment with the lack of exciting or innovative options. This issue is significantly impacting the overall dining experience, with a 15% decrease in positive mentions of food quality compared to the previous quarter. Furthermore, data shows a 20% decline in repeat visits among customers who left reviews mentioning food quality issues.",
                "solution": "Implement a comprehensive menu revamp focusing on flavor enhancement and innovation:\n                1. Conduct a two-day flavor profiling workshop with the kitchen staff, led by a culinary consultant, to identify specific areas for improvement in current dishes.\n                2. Introduce a new spice and herb blend to enhance flavors.\n                3. Develop a seasonal menu that incorporates fresh, locally sourced ingredients to create exciting and innovative options.\n                4. Consider partnering with a food blogger or influencer to promote the new menu and increase visibility.",
                "urgency": 5
            },
            {
                "title": "Service Efficiency Improvement",
                "problem": "Analysis of customer reviews indicates a recurring theme of dissatisfaction with service speed. Approximately 55% of negative reviews highlight long wait times, with 30% specifically mentioning that staff seemed overwhelmed during peak hours. This has led to a noticeable drop in customer satisfaction ratings, with a 25% increase in complaints related to service delays over the last quarter.",
                "solution": "To enhance service efficiency, consider the following actions:\n                1. Implement a staff training program focused on time management and customer engagement during busy periods.\n                2. Increase staffing levels during peak hours to ensure adequate coverage and reduce wait times.\n                3. Introduce a digital ordering system to streamline the ordering process and minimize delays.",
                "urgency": 4
            },
            {
                "title": "Food Quality Enhancement",
                "problem": "Customer feedback reveals a significant concern regarding food quality, with 60% of reviews mentioning issues such as blandness and lack of freshness. This has resulted in a 30% decrease in positive feedback about food quality over the past six months, indicating a pressing need for improvement in this area.",
                "solution": "To address food quality concerns, the following steps are recommended:\n                1. Revise the menu to include more flavorful and fresh ingredients, focusing on seasonal produce.\n                2. Conduct regular taste tests and gather feedback from customers to ensure dishes meet quality standards.\n                3. Collaborate with local suppliers to source high-quality ingredients and enhance the overall dining experience.",
                "urgency": 3
            },
            {
                "title": "Location Accessibility Improvement",
                "problem": "Feedback indicates that customers find the restaurant's location difficult to access, with 40% of reviews mentioning issues related to parking and public transport. This has led to a decline in foot traffic, with a 15% drop in new customer visits over the last quarter.",
                "solution": "To improve accessibility, consider the following actions:\n                1. Partner with local transport services to provide shuttle options for customers.\n                2. Enhance signage and provide clear directions on the website and social media platforms.\n                3. Explore options for additional parking spaces or validate parking for customers.",
                "urgency": 2
            }
        ]
    }
    """

    payload = {
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": "You are an AI assistant that generates detailed insights based on customer reviews. Provide your response in a valid JSON format, following the structure of the example provided. Generate exactly 3 highlights, 3 lowlights, and at least 4 highly detailed insights. For each insight, include an urgency level from 1 to 6, where 1 is least urgent and 6 is most urgent.",
                    }
                ],
            },
            {
                "role": "user",
                "content": f"""Here's an example of the JSON format to follow. Generate exactly 3 highlights (most positive aspects) and 3 lowlights (most negative aspects) of the business, always based on the reviews provided. Also, generate at least 4 highly detailed insights that are actionable and specific to the business. Don't be generic, and make them very detailed and comprehensive. Include an urgency level (1-6) for each insight:

{example_json}

Now, generate insights based on the following customer reviews, using the same JSON structure. Remember to provide exactly 3 highlights, 3 lowlights, and at least 4 detailed insights with urgency levels:

{prompt}""",
            },
        ],
        "temperature": 0.3,  # Lowered temperature for more consistent and detailed output
    }

    try:
        response = requests.post(ENDPOINT, headers=headers, json=payload)
        response.raise_for_status()
        json_response = (
            response.json()
            .get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
        return json.loads(json_response)  # Parse the JSON response
    except requests.RequestException as e:
        raise Exception(f"Failed to generate insights. Error: {e}")


def generate_insights_for_company(company_id):
    # Fetch all reviews for the company and convert to a list
    all_reviews = list(ReviewModel.fetch_reviews_by_company_id(company_id))

    # Sort reviews by date (newest first) and take the most recent 50
    reviews = sorted(all_reviews, key=lambda review: review.review_date, reverse=True)[
        :50
    ]

    if not reviews:
        return "No reviews found for this company."

    # Prepare the prompt for insight generation
    review_texts = [review.review_text for review in reviews]
    prompt = "\n\n".join(review_texts)

    # Generate insights using Azure OpenAI
    insights = generate_insights_using_azure_openai(prompt)
    from datetime import datetime

    # Add last_sync field to insights
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    insights["last_sync"] = current_time
    CompanyModel.update_insights(company_id, insights)

    return insights
