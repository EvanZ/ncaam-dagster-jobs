import os

from dagster import (
    AssetSelection, define_asset_job, 
    job, config_from_files)
from ..assets.constants import (
    DAILY, DAILY_WOMEN, MODELS, MODELS_WOMEN, TOP_LINES, 
    TOP_LINES_WOMEN, RANKINGS, RANKINGS_WOMEN, SEASONAL, WEB_EXPORT)
from ..partitions import daily_partition
from ..assets.ops import create_drop_table_op, create_backup_table_op, create_restore_table_op


seasonal_update_job = define_asset_job(
    name="seasonal_update",
    selection=AssetSelection.groups(SEASONAL)
)

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

models_update_job_women = define_asset_job(
    name="models_update_women",
    selection=AssetSelection.groups(MODELS_WOMEN)
    )

# Jobs that skip scraping and only process already-downloaded files
daily_backfill_job = define_asset_job(
    name="daily_backfill",
    selection=AssetSelection.groups(DAILY) - AssetSelection.assets("daily_scoreboard_file", "game_summary_files"),
    partitions_def=daily_partition,
)

daily_backfill_job_women = define_asset_job(
    name="daily_backfill_women",
    selection=AssetSelection.groups(DAILY_WOMEN) - AssetSelection.assets("daily_scoreboard_file_women", "game_summary_files_women"),
    partitions_def=daily_partition,
)

run_config_top_lines = os.environ["CONFIG_PATH"]
top_lines_job = define_asset_job(
    name="top_lines_reports",
    selection=AssetSelection.groups(TOP_LINES),
    config=config_from_files(config_files=[run_config_top_lines])
)

run_config_top_lines_women = os.environ["CONFIG_PATH_WOMEN"]
top_lines_job_women = define_asset_job(
    name="top_lines_reports_women",
    selection=AssetSelection.groups(TOP_LINES_WOMEN),
    config=config_from_files(config_files=[run_config_top_lines_women])
)

season_report_job = define_asset_job(
    name="season_reports",
    selection=AssetSelection.groups(RANKINGS),
    config=config_from_files(config_files=[os.environ["CONFIG_SEASON_RANKINGS_PATH"]])
)

season_report_job_women = define_asset_job(
    name="season_report_women",
    selection=AssetSelection.groups(RANKINGS_WOMEN),
    config=config_from_files(config_files=[os.environ["CONFIG_SEASON_RANKINGS_PATH_WOMEN"]])
)

# Web export job - generates JSON for the Vue.js frontend
web_export_job = define_asset_job(
    name="web_export",
    selection=AssetSelection.groups(WEB_EXPORT) - AssetSelection.assets("web_prospects_json")
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
drop_stage_box_stat_adjustment_factors_women = create_drop_table_op("stage_box_stat_adjustment_factors_women")
drop_stage_shot_type_adjustment_factors_women = create_drop_table_op("stage_shot_type_adjustment_factors_women")
backup_stage_box_stat_adjustment_factors = create_backup_table_op("stage_box_stat_adjustment_factors")
backup_stage_shot_type_adjustment_factors = create_backup_table_op("stage_shot_type_adjustment_factors")
backup_stage_box_stat_adjustment_factors_women = create_backup_table_op("stage_box_stat_adjustment_factors_women")
backup_stage_shot_type_adjustment_factors_women = create_backup_table_op("stage_shot_type_adjustment_factors_women")
restore_stage_box_stat_adjustment_factors = create_restore_table_op("stage_box_stat_adjustment_factors")
restore_stage_shot_type_adjustment_factors = create_restore_table_op("stage_shot_type_adjustment_factors")
restore_stage_box_stat_adjustment_factors_women = create_restore_table_op("stage_box_stat_adjustment_factors_women")
restore_stage_shot_type_adjustment_factors_women = create_restore_table_op("stage_shot_type_adjustment_factors_women")
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

@job
def backup_models_job():
    backup_stage_box_stat_adjustment_factors()
    backup_stage_shot_type_adjustment_factors()
    backup_stage_box_stat_adjustment_factors_women()
    backup_stage_shot_type_adjustment_factors_women()

@job
def cleanup_models_job():
    drop_stage_box_stat_adjustment_factors()
    drop_stage_shot_type_adjustment_factors()
    drop_stage_box_stat_adjustment_factors_women()
    drop_stage_shot_type_adjustment_factors_women()

@job
def restore_models_job():
    restore_stage_box_stat_adjustment_factors()
    restore_stage_shot_type_adjustment_factors()
    restore_stage_box_stat_adjustment_factors_women()
    restore_stage_shot_type_adjustment_factors_women()
