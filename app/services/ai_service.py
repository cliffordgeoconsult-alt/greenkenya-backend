# app/services/ai_service.py

from openai import OpenAI
import json

client = OpenAI()

def generate_ai_insight(domain: str, data: dict):
    prompt = f"""
    You are a {domain} environmental intelligence system.

    Data:
    {data}

    Return JSON:
    {{
      "summary": "...",
      "key_issue": "...",
      "trend": "...",
      "recommendation": "..."
    }}
    """

    response = client.chat.completions.create(
        model="gpt-5",
        temperature=0.3,
        messages=[{"role": "user", "content": prompt}]
    )

    return json.loads(response.choices[0].message.content)