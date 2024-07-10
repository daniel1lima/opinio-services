import openai

openai.api_key = 'your-openai-api-key'

def generate_business_insights(summary: str):

    prompt = f"Given the following summary of a business's problems, provide four insights on potential solutions:\n\n{summary}\n\nInsight 1:"

    response = openai.Completion.create(
        engine="gpt-3.5-turbo", 
        prompt=prompt,
        max_tokens=300,  
        n=1,
        stop=None,
        temperature=0.7,
    )
    insights_text = response.choices[0].text.strip()
    insights = insights_text.split("\nInsight ")
    insight1 = insights[1].strip() if len(insights) > 1 else ""
    insight2 = insights[2].strip() if len(insights) > 2 else ""
    insight3 = insights[3].strip() if len(insights) > 3 else ""
    insight4 = insights[4].strip() if len(insights) > 4 else ""

    return insight1, insight2, insight3, insight4

summary_of_problems = "The business is facing a decline in customer satisfaction, high employee turnover, increased operational costs, and low brand awareness."

insight1, insight2, insight3, insight4 = generate_business_insights(summary_of_problems)