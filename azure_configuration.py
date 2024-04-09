import msal
from typing import (
    Any,
    Dict,
    Union
)


def initialize_azure_ad_app(
    CLIENT_ID: str,
    CLIENT_SECRET: str,
    AUTHORITY: str,
    SCOPES: str
) -> tuple[Any, Dict[str, Union[str, int]]]:
    """
    Initialize Azure AD app and acquire token for Microsoft Azure

    Args:
        CLIENT_ID (string)
        CLIENT_SECRET (string)
        AUTHORITY (string)
        SCOPES (string)

    Returns:
        app (msal.application.ConfidentialClientApplication)
        result (Dict): Information regarding the access_token
    """

    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=AUTHORITY
    )

    result = app.acquire_token_for_client(SCOPES)

    return app, result
