import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


class LLMReasoner:
    def build_context(self, results):
        chunks = []
        for i, r in enumerate(results, start=1):
            chunks.append(
                f"""Complaint {i}:
Product: {r.get('title', '')}
Brand: {r.get('brand', '')}
Category: {r.get('category', '')}
Complaint Type: {r.get('complaint_type', '')}
Complaint Subtype: {r.get('complaint_subtype', '')}
Summary: {r.get('summary', '')}
Signal Score: {r.get('signal_score', '')}
Review Date: {r.get('review_date', '')}
"""
            )
        return "\n".join(chunks)

    def generate(self, query, results):

        context = self.build_context(results)
        num_complaints = len(results)

        prompt = f"""
    You are an expert product operations analyst.

    Your job is to convert customer complaints into decision intelligence.

    User Query:
    {query}

    Total Complaints Analyzed: {num_complaints}

    Customer Complaint Evidence:
    {context}

    Rules:
    - Use ONLY the evidence provided
    - Do NOT invent facts
    - Highlight recurring patterns across complaints
    - Be concise and business-focused

    Return EXACTLY in this format:

    Issue Summary:
    - Based on {num_complaints} complaints analyzed, ...

    Likely Recurring Pattern:
    - ...

    Root Cause Hypothesis:
    - ...

    Business Impact:
    - ...

    Recommended Actions:
    - ...
    """

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )

        return response.choices[0].message.content