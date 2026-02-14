"""
Setup Gmail Auth
================
One-time script to authenticate with Gmail and store the OAuth2 refresh token.

Run this once before using the email pipeline:
    python setup_gmail_auth.py

This will:
1. Open your browser for Google OAuth2 consent
2. Request read + modify access to your Gmail
3. Save the refresh token to token.json

Prerequisites:
    - Download credentials.json from Google Cloud Console
    - Place it in this project's root directory
    - Enable the Gmail API in your Google Cloud project

Steps to get credentials.json:
    1. Go to https://console.cloud.google.com
    2. Create a new project (or select existing)
    3. Go to APIs & Services > Library
    4. Search for "Gmail API" and enable it
    5. Go to APIs & Services > Credentials
    6. Click "Create Credentials" > "OAuth client ID"
    7. Select "Desktop app" as the application type
    8. Download the JSON file and save it as credentials.json
"""

import sys
from pathlib import Path

# Add project root to path
_PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_PROJECT_ROOT))

from config import get_gmail_credentials_path, get_gmail_token_path
from email_pipeline.gmail_auth import authenticate, SCOPES


def main():
    credentials_path = get_gmail_credentials_path()
    token_path = get_gmail_token_path()

    print("Gmail OAuth2 Setup")
    print("=" * 50)
    print(f"Credentials file: {credentials_path}")
    print(f"Token file:       {token_path}")
    print()

    if not credentials_path.exists():
        print("ERROR: credentials.json not found!")
        print()
        print("To set up Gmail API access:")
        print("  1. Go to https://console.cloud.google.com")
        print("  2. Create a project and enable the Gmail API")
        print("  3. Create OAuth2 Desktop App credentials")
        print("  4. Download the JSON and save it as:")
        print(f"     {credentials_path}")
        sys.exit(1)

    if token_path.exists():
        print("Existing token found. Re-authenticating...")
        print()

    print("Opening browser for Google sign-in...")
    print("(Grant read + modify access to your Gmail)")
    print()

    try:
        creds = authenticate(credentials_path, token_path)
        print()
        print("Authentication successful!")
        print(f"Token saved to: {token_path}")
        print()
        print("Scopes granted:")
        for scope in SCOPES:
            print(f"  - {scope}")
        print()
        print("You can now run the email pipeline.")
    except Exception as e:
        print(f"ERROR: Authentication failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
