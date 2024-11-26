import os

from dagster import AssetSelection, define_asset_job, job, config_from_files, EnvVar
from ..assets.constants import DAILY, DAILY_WOMEN, MODELS, TOP_LINES, RANKINGS
from ..partitions import daily_partition
from ..assets.ops import create_drop_table_op


daily_update_job = define_asset_job(
    name="daily_update",
    selection=AssetSelection.groups(DAILY).required_multi_asset_neighbors(),
    partitions_def=daily_partition,
)

daily_update_job_women = define_asset_job(
    name="daily_update_women",
    selection=AssetSelection.groups(DAILY_WOMEN).required_multi_asset_neighbors(),
    partitions_def=daily_partition,
)

models_update_job = define_asset_job(
    name="models_update",
    selection=AssetSelection.groups(MODELS)
    )

run_config = os.environ["CONFIG_PATH"]
top_lines_job = define_asset_job(
    name="top_lines_reports",
    selection=AssetSelection.groups(TOP_LINES),
    config=config_from_files(config_files=[run_config])
)

season_report_job = define_asset_job(
    name="season_reports",
    selection=AssetSelection.groups(RANKINGS),
    config=config_from_files(config_files=['/Users/evanzamir/projects/ncaam-data-pipelines/dagster-jobs/dagster_jobs/run_config_rankings.yaml'])
)

drop_stage_daily_scoreboard = create_drop_table_op("stage_daily_scoreboard")
drop_stage_game_logs = create_drop_table_op("stage_game_logs")
drop_stage_player_lines = create_drop_table_op("stage_player_lines")
drop_stage_plays = create_drop_table_op("stage_plays")
drop_stage_conferences = create_drop_table_op("stage_conferences")
drop_stage_box_stat_adjustment_factors = create_drop_table_op("stage_box_stat_adjustment_factors")
drop_stage_player_shots_by_game = create_drop_table_op("stage_player_shots_by_game")
drop_stage_player_assists_by_game = create_drop_table_op("stage_player_assists_by_game")
drop_stage_rsci_rankings = create_drop_table_op("stage_rsci_rankings")
drop_stage_shot_type_adjustment_factors = create_drop_table_op("stage_shot_type_adjustment_factors")
drop_stage_teams = create_drop_table_op("stage_teams")
drop_stage_top_lines = create_drop_table_op("stage_top_lines")
drop_stage_players = create_drop_table_op("stage_players")

# Now, use those ops within the job definition
@job
def cleanup_job():
    drop_stage_daily_scoreboard()
    drop_stage_game_logs()
    drop_stage_player_lines()
    drop_stage_plays()
    drop_stage_conferences()
    drop_stage_box_stat_adjustment_factors()
    drop_stage_player_shots_by_game()
    drop_stage_player_assists_by_game()
    drop_stage_rsci_rankings()
    drop_stage_shot_type_adjustment_factors()
    drop_stage_teams()
    drop_stage_top_lines()
    drop_stage_players()