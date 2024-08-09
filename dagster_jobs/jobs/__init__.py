from dagster import AssetSelection, define_asset_job, job

from ..partitions import daily_partition
from ..assets.espn import create_drop_table_op

daily_update_job = define_asset_job(
    name="daily_update_job",
    selection=AssetSelection.all().required_multi_asset_neighbors(),
    partitions_def=daily_partition,
)

drop_stage_daily_scoreboard = create_drop_table_op("stage_daily_scoreboard")
drop_stage_game_logs = create_drop_table_op("stage_game_logs")
drop_stage_player_lines = create_drop_table_op("stage_player_lines")
drop_stage_plays = create_drop_table_op("stage_plays")

# Now, use those ops within the job definition
@job
def cleanup_job():
    drop_stage_daily_scoreboard()
    drop_stage_game_logs()
    drop_stage_player_lines()
    drop_stage_plays()