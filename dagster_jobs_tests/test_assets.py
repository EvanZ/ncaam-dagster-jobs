import os
import warnings

import pytest
from dagster import build_asset_context, resource
from dagster_duckdb import DuckDBResource
from loguru import logger

from dagster_jobs.assets.espn import stage_daily_scoreboard
from dagster_jobs_tests.test_resources import mock_context

# Configure loguru to log to a file
logger.add("test_output.log", level="INFO", format="{time} - {level} - {message}")

@pytest.fixture(scope='function', autouse=True)
def set_test_env_vars():
    os.environ['DAILY_GAME_SUMMARIES_PATH'] = 'dagster-jobs/data/raw/daily_game_summaries_'

@resource
def mock_duckdb_resource(_):
    return DuckDBResource(database=":memory:")

def test_stage_daily_scoreboard():
    context = build_asset_context(partition_key='2024-03-21')
    res = stage_daily_scoreboard(context, mock_duckdb_resource)
    assert os.environ['DAILY_GAME_SUMMARIES_PATH'] == 'dagster-jobs/data/raw/daily_game_summaries_'
    assert res.metadata['num_games'] == 16

# Run the test
if __name__ == "__main__":
    pytest.main(["-v", __file__])
