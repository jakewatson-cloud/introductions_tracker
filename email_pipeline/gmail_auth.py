"""
Gmail Auth
==========
OAuth2 authentication for the Gmail API.

Manages token storage and automatic refresh.
"""

import logging
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

# Gmail API scopes
# readonly: read emails and metadata
# modify: add labels to processed emails
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]


def authenticate(credentials_path: Path, token_path: Path) -> Credentials:
    """Load or create OAuth2 credentials.

    If a valid token exists, loads and refreshes it.
    Otherwise, runs the interactive OAuth2 consent flow.

    Parameters
    ----------
    credentials_path : Path
        Path to the Google OAuth2 client credentials.json file
        (downloaded from Google Cloud Console).
    token_path : Path
        Path to store/load the token.json refresh token.

    Returns
    -------
    Credentials
        Valid Google OAuth2 credentials.

    Raises
    ------
    FileNotFoundError
        If credentials.json does not exist and no token.json is available.
    """
    creds = None

    # Load existing token if available
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
        logger.info("Loaded existing token from %s", token_path)

    # Refresh or re-authenticate
    if creds and creds.expired and creds.refresh_token:
        logger.info("Token expired, refreshing...")
        creds.refresh(Request())
        # Save refreshed token
        token_path.write_text(creds.to_json())
        logger.info("Token refreshed and saved")

    elif not creds or not creds.valid:
        if not credentials_path.exists():
            raise FileNotFoundError(
                f"credentials.json not found at {credentials_path}\n"
                "Download it from Google Cloud Console:\n"
                "  1. Go to https://console.cloud.google.com/apis/credentials\n"
                "  2. Create an OAuth2 Desktop App client\n"
                "  3. Download the JSON and save it as credentials.json"
            )

        logger.info("No valid token, starting OAuth2 consent flow...")
        flow = InstalledAppFlow.from_client_secrets_file(
            str(credentials_path), SCOPES
        )
        creds = flow.run_local_server(port=0)

        # Save the token for future runs
        token_path.write_text(creds.to_json())
        logger.info("New token saved to %s", token_path)

    return creds


def get_gmail_service(credentials_path: Path, token_path: Path):
    """Build and return an authenticated Gmail API service.

    Parameters
    ----------
    credentials_path : Path
        Path to credentials.json.
    token_path : Path
        Path to token.json.

    Returns
    -------
    googleapiclient.discovery.Resource
        Authenticated Gmail API service object.
    """
    creds = authenticate(credentials_path, token_path)
    service = build("gmail", "v1", credentials=creds)
    return service
