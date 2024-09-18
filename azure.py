import os
import requests
import base64

# Configuration
API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
# IMAGE_PATH = "YOUR_IMAGE_PATH"
# encoded_image = base64.b64encode(open(IMAGE_PATH, 'rb').read()).decode('ascii')
headers = {
    "Content-Type": "application/json",
    "api-key": API_KEY,
}

# Payload for the request
payload = {
    "messages": [
        {
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": "You are an AI assistant that helps people find information.",
                }
            ],
        }
    ],
    "temperature": 0.7,
    "top_p": 0.95,
    "max_tokens": 800,
}

ENDPOINT = "https://opinio.openai.azure.com/openai/deployments/4o-mini/chat/completions?api-version=2024-02-15-preview"

# Send request
try:
    response = requests.post(ENDPOINT, headers=headers, json=payload)
    response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
except requests.RequestException as e:
    raise SystemExit(f"Failed to make the request. Error: {e}")

# Handle the response as needed (e.g., print or process)
print(response.json())
