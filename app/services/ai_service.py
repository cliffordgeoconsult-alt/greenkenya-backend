# app/services/ai_service.py

from openai import OpenAI
import json
import os

def get_client():
    """
    Create OpenAI client safely at runtime.
    """
    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        raise ValueError("OPENAI_API_KEY not found. Check your .env file.")

    return OpenAI(api_key=api_key)


def generate_ai_insight(domain: str, data: list):
    client = get_client()

    prompt = f"""
You are a {domain} environmental intelligence system.

Analyze the data and explain:
- What is happening
- Why it matters
- What should be done

DATA:
{json.dumps(data)}

Return STRICT JSON:
{{
  "summary": "...",
  "key_issue": "...",
  "trend": "...",
  "recommendation": "..."
}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0.3,
            messages=[{"role": "user", "content": prompt}]
        )

        return json.loads(response.choices[0].message.content)

    except Exception as e:
        return {
            "summary": "AI unavailable",
            "key_issue": "Error generating insight",
            "trend": "unknown",
            "recommendation": str(e)
        }