#!/usr/bin/env python3
"""
Fetch data from 360Learning and write a summary JSON file for the widget.

Env vars required:
    CLIENT_ID       - 360Learning API v2 client ID
    CLIENT_SECRET   - 360Learning API v2 client secret
Optional:
    BASE_URL        - Defaults to https://app.360learning.com (EU)
                      Use https://app.us.360learning.com for US
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
BASE_URL = os.environ.get("BASE_URL", "https://app.360learning.com")

OUTPUT_PATH = Path(__file__).parent / "data.json"
API_VERSION = "v2.0"


def get_access_token() -> str:
    """Exchange client credentials for a bearer token."""
    resp = requests.post(
        f"{BASE_URL}/api/v2/oauth2/token",
        headers={
            "accept": "application/json",
            "content-type": "application/json",
        },
        json={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_courses(token: str, limit: int = 100) -> list:
    """
    Fetch a page of courses. Swap this function (and the endpoint) for
    any other dataset you want to summarise — e.g. /api/v2/users,
    /api/v2/groups, /api/v2/paths.
    Remember to grant the matching scope to your API credential.
    """
    resp = requests.get(
        f"{BASE_URL}/api/v2/courses",
        headers={
            "accept": "application/json",
            "360-api-version": API_VERSION,
            "authorization": f"Bearer {token}",
        },
        params={"limit": limit},
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    # The API wraps list results in an object; be tolerant of both shapes.
    if isinstance(body, dict):
        return body.get("data", body.get("results", []))
    return body


def summarise(courses: list) -> dict:
    """Shape the raw response into something the widget can render cheaply."""
    total = len(courses)

    # Count by language if the field is present — adjust to your data.
    language_counts: dict = {}
    for c in courses:
        lang = c.get("language") or c.get("lang") or "unknown"
        language_counts[lang] = language_counts.get(lang, 0) + 1

    # Keep only the fields the widget needs, so data.json stays small.
    recent = [
        {
            "id": c.get("_id") or c.get("id"),
            "name": c.get("name") or c.get("title") or "(untitled)",
            "language": c.get("language") or c.get("lang"),
            "createdAt": c.get("createdAt") or c.get("created_at"),
        }
        for c in courses[:25]
    ]

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totalCourses": total,
        "byLanguage": language_counts,
        "recent": recent,
    }


def main() -> int:
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: CLIENT_ID and CLIENT_SECRET must be set.", file=sys.stderr)
        return 1

    try:
        print(f"Getting token from {BASE_URL} ...")
        token = get_access_token()
        print("Fetching courses ...")
        courses = fetch_courses(token)
        print(f"Got {len(courses)} courses.")
        summary = summarise(courses)
    except requests.exceptions.HTTPError as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        print(f"Response body: {exc.response.text}", file=sys.stderr)
        return 1
    except requests.exceptions.RequestException as exc:
        print(f"Request failed: {exc}", file=sys.stderr)
        return 1

    OUTPUT_PATH.write_text(json.dumps(summary, indent=2))
    print(f"Wrote {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
