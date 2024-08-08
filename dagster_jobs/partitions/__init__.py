import os
from datetime import datetime

from dagster import DailyPartitionsDefinition

daily_partition = DailyPartitionsDefinition(
    start_date=os.environ['START_DATE'],
    end_date=os.environ['END_DATE']
)