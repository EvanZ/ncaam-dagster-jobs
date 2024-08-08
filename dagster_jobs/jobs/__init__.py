from dagster import AssetSelection, define_asset_job

from ..partitions import daily_partition

daily_update_job = define_asset_job(
    name="daily_update_job",
    selection=AssetSelection.all().required_multi_asset_neighbors(),
    partitions_def=daily_partition,
)
