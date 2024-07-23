import datetime
import json
import os
import re
import requests
import time
from pprint import pprint
from datetime import date, datetime

import duckdb
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource
from loguru import logger
import numpy as np
import pandas as pd

from .constants import DATE_FORMAT, DAILY_SCOREBOARD_TEMPLATE_PATH
from ..partitions import daily_partition

scoreboard_url = lambda date_str: f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&groups=50&limit=150"

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

@asset(
    partitions_def=daily_partition,
)
def daily_scoreboard_file(context: AssetExecutionContext) -> None:
    """
    downloads scoreboard of games partitioned by day and writes them to disk
    """
    partition_date_str = context.partition_key
    date_obj = datetime.strptime(partition_date_str, "%Y-%m-%d")
    date_str = date_obj.strftime(DATE_FORMAT)
    scoreboard_url_complete = scoreboard_url(date_str)
    scoreboard_info = fetch_data(scoreboard_url_complete)

    os.makedirs("data", exist_ok=True)
    with open(DAILY_SCOREBOARD_TEMPLATE_PATH(date_str), "w") as f:
        json.dump(scoreboard_info, f)

    return MaterializeResult(
        metadata={
            "num_games": len(scoreboard_info["events"])
        }
    )

@asset(
    deps=["daily_scoreboard_file"],
    partitions_def=daily_partition,
)
def stage_games(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """
    loads games from downloaded files into duckdb | partitioned by day
    """

    partition_date_str = context.partition_key
    logger.info(partition_date_str)
    date_obj = datetime.strptime(partition_date_str, "%Y-%m-%d")
    logger.info(date_obj)
    date_str = date_obj.strftime(DATE_FORMAT)
    logger.info(date_str)

    create_table_query = """
        create table if not exists stage_games (
            id STRING,
            date STRING,
            name STRING,
            short_name STRING,
            completed BOOLEAN
        );
    """

    insert_stage_games_query = f"""
        insert into stage_games 
            select 
                games['id'] as id, 
                games['date'] as date, 
                games['name'] as name, 
                games['shortName'] as short_name, 
                games['status']['type']['completed'] as completed 
            from (
                select 
                    unnest(events) as games 
                from read_json('{DAILY_SCOREBOARD_TEMPLATE_PATH(date_str)}')
            )
            where games['status']['type']['completed'] is true
            returning id;
    """
    with database.get_connection() as conn:
        conn.execute(create_table_query)
        res = conn.execute(insert_stage_games_query).fetchnumpy()

    return MaterializeResult(
        metadata={
            "num_games": len(res['id'])
        }
    )
