import openai

def generate_response(review):
    openai.api_key = 'sk-proj-dGyDnojE2BvnUMJfGKdpT3BlbkFJgU2vzYPHUOw56yozrFjo'

    response = openai.Completion.create(
        engine="gpt-3.5-turbo",  # or choose another model based on your preference
        prompt=f"Business review: \"{review}\"\n\nBusiness owner's response:",
        max_tokens=150,
        temperature=0.7
    )

    return response.choices[0].text.strip()

# Example usage
review_text = "The bathrooms are a mess, Jack is a great instructor but the prices of the membership are quite high."
response_text = generate_response(review_text)
print("Generated Response:", response_text)

