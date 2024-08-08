import requests
from loguru import logger

def fetch_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        return response.json()  # Assuming JSON response
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        # Handle specific HTTP errors or propagate the exception
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request error occurred: {req_err}")
        # Handle connection-related errors or propagate the exception
    except ValueError as val_err:
        logger.error(f"Value error occurred while parsing JSON: {val_err}")
        # Handle JSON parsing errors
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
        # Handle any other unexpected errors
