import msal
import pandas as pd
import requests
from typing import (
    Any,
    Dict,
    List,
    Tuple
)


def fetch_planner_data(
    app: msal.ConfidentialClientApplication,
    result: (dict[str | Any, Any | str] | Any | None),
    PLAN_ID: str
) -> pd.DataFrame:
    """
    Defines the Planner tasks endpoint for the specified plan and
    makes a request to retrieve Planner tasks

    Args:
        app (msal.application.ConfidentialClientApplication)
        result (Dict)
        PLAN_ID (str)

    Returns:
        tasks_normalized (Pandas DataFrame)
    """

    if 'access_token' in result:
        access_token = result['access_token']

        planner_endpoint = (
            f'https://graph.microsoft.com/v1.0/planner/plans/{PLAN_ID}/tasks'
        )

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(planner_endpoint, headers=headers)
        response.raise_for_status()
        planner_tasks = response.json()

        return planner_tasks

    else:
        raise Exception('Failed to obtain access token')


def fetch_planner_buckets(
    app: msal.ConfidentialClientApplication,
    result: (dict[str | Any, Any | str] | Any | None),
    PLAN_ID: str
) -> Dict[str, str]:
    """
    Defines the Planner buckets endpoint for the specified plan,
    makes a request to retrieve Planner buckets
    and creates a dictionary to map bucket IDs to names

    Args:
        app (msal.application.ConfidentialClientApplication)
        result (Dict)
        PLAN_ID (str)

    Returns:
        bucket_name_mapping (Dict)
    """

    if 'access_token' in result:
        access_token = result['access_token']

        planner_endpoint = (
            f'https://graph.microsoft.com/v1.0/planner/plans/{PLAN_ID}/buckets'
        )

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(planner_endpoint, headers=headers)
        response.raise_for_status()
        planner_buckets = response.json()

        bucket_name_mapping = {
            bucket['id']: bucket['name'] for bucket in planner_buckets['value']
        }

        return bucket_name_mapping
    else:
        raise Exception('Failed to obtain access token for fetching buckets')


def fetch_categories_name(
    app: msal.ConfidentialClientApplication,
    result: (dict[str | Any, Any | str] | Any | None),
    PLAN_ID: str
) -> List[Tuple[str, str]]:

    if 'access_token' in result:
        access_token = result['access_token']

        planner_endpoint = (
            f'https://graph.microsoft.com/v1.0/planner/plans/{PLAN_ID}/details'
        )

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(planner_endpoint, headers=headers)
        response.raise_for_status()
        planner_categories = response.json()

        category_name_mapping = [(category, name) for category, name in planner_categories["categoryDescriptions"].items()]

        return category_name_mapping
    else:
        raise Exception('Failed to obtain access token for fetching buckets')
