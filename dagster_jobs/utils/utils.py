import requests
from loguru import logger

def fetch_data(url, context):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        if response.json() == {}:
            raise ValueError(f"empty object for {url}")
        return response.json()  # Assuming JSON response
    except requests.exceptions.HTTPError as http_err:
        context.log.error(f"HTTP error occurred: {http_err} for {url}")
        raise http_err
        # Handle specific HTTP errors or propagate the exception
    except requests.exceptions.RequestException as req_err:
        context.log.error(f"Request error occurred: {req_err} for {url}")
        raise req_err
        # Handle connection-related errors or propagate the exception
    except ValueError as val_err:
        context.log.error(f"Value error occurred while parsing JSON: {val_err} for {url}")
        raise val_err
        # Handle JSON parsing errors
    except Exception as err:
        context.log.error(f"Other error occurred: {err} for {url}")
        raise err
        # Handle any other unexpected errors
