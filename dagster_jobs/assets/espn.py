import datetime
import json
import os
import re
from glob import glob
from datetime import datetime
from typing import Generator, Literal
import warnings

import dagster
from dagster import (
    asset, 
    op, 
    AssetExecutionContext, 
    MaterializeResult, 
    MetadataValue, 
    AssetCheckResult, 
    AssetCheckSpec, 
    AssetsDefinition,
    Output, 
    RetryPolicy,
)
from dagster_duckdb import DuckDBResource
from duckdb import InvalidInputException
import fasteners
import numpy
import pandas
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import RidgeCV

from . import queries
from .constants import (
    DATE_FORMAT, 
    ROSTER_URL, 
    SCOREBOARD_URL_TEMPLATE, 
    GAME_SUMMARY_URL_TEMPLATE, 
    TEAMS_URL, 
    CONFERENCES_URL,
    KENPOM_URL
    )
from ..partitions import daily_partition
from ..resources import LocalFileStorage
from ..utils.utils import fetch_data

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

def create_drop_table_op(table_name: str):
    @op(name=f"drop_{table_name}_table")
    def drop_table_op(database: DuckDBResource) -> Output:
        """
        Drop a staging table and return metadata with the number of rows dropped.
        """
        with database.get_connection() as conn:
            try:
                # Check if the table exists and count rows
                result = conn.execute(f"SELECT COUNT(*) as row_count FROM {table_name}").fetchone()
                row_count = result[0] if result else 0
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            except InvalidInputException:
                row_count = 0

        return Output(
            value=row_count,
            metadata={
                "rows_dropped": MetadataValue.int(row_count)
            }
        )

    return drop_table_op

@asset(
    group_name="update_manually"
)
def stage_kenpom(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage latest kenpom rankings
    """
    path = os.path.join(storage.filepath, "kenpom", "kenpom.csv")
    context.log.info(path)

    with database.get_connection() as conn:
        conn.execute("""
                     create table if not exists stage_kenpom (
                        rank INT,
                        team STRING PRIMARY KEY,
                        ortg DOUBLE,
                        drtg DOUBLE,
                        year INT
                     );
                     """)
        df = conn.execute(queries.insert_table_stage_kenpom(path)).df()        
 
    return MaterializeResult(
        metadata={
            "path": path,
            "df": MetadataValue.md(df.to_markdown()),
            "num_teams": MetadataValue.int(len(df))
        }
    )

@asset(
    group_name="update_annual"
)
def teams_file(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    downloads the teams from the espn api
    """
    path = os.path.join(storage.filepath, "teams")
    os.makedirs(path, exist_ok=True)

    teams_info = fetch_data(TEAMS_URL, context)
    with open(os.path.join(path, "teams.json"), "w") as f:
        json.dump(teams_info, f)

    with database.get_connection() as conn:
        df = conn.execute(queries.teams_metadata(os.path.join(path, "teams.json"))).df()
        context.log.info(df.head(n=10))

    return MaterializeResult(
        metadata={
            "path": path,
            "num_teams": MetadataValue.int(len(df)),
            "sample": MetadataValue.md(df.sample(n=25)[['shortConference','id','displayName']].to_markdown())
        }
    )

@asset(
    group_name="update_annual"
)
def conferences_file(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    downloads the teams from the espn api
    """
    path = os.path.join(storage.filepath, "conferences")
    os.makedirs(path, exist_ok=True)

    teams_info = fetch_data(CONFERENCES_URL, context)
    with open(os.path.join(path, "conferences.json"), "w") as f:
        json.dump(teams_info, f)

    with database.get_connection() as conn:
        df = conn.execute(queries.conferences_metadata(os.path.join(path, "conferences.json"))).df()
        context.log.info(df.head(n=10))

    return MaterializeResult(
        metadata={
            "path": path,
            "num_confs": MetadataValue.int(len(df)),
            "sample": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=[conferences_file],
    group_name="update_annual"    
)
def stage_conferences(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stages the conference data
    """
    path = os.path.join(storage.filepath, "conferences", "conferences.json")
    context.log.info(f'path: {path}')

    if not os.path.exists(path):
        raise Exception("path/to/conferences does not exist...aborting further materialization")
    
    with database.get_connection() as conn:
        conn.execute(queries.create_table_stage_conferences())
        context.log.info(queries.insert_table_stage_conferences(path))
        df = conn.execute(queries.insert_table_stage_conferences(path)).df()

    return MaterializeResult(
        metadata={
            "num_rows": len(df),
            "df": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=[conferences_file],
    group_name="update_annual"    
)
def stage_teams(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stages the teams data
    """
    path = os.path.join(storage.filepath, "teams", "teams.json")
    context.log.info(f'path: {path}')

    if not os.path.exists(path):
        raise Exception("path/to/teams does not exist...aborting further materialization")
    
    with database.get_connection() as conn:
        conn.execute("drop table if exists stage_teams;")
        conn.execute(queries.create_table_stage_teams())
        context.log.info(queries.insert_table_stage_teams(path))
        df = conn.execute(queries.insert_table_stage_teams(path)).df()

    return MaterializeResult(
        metadata={
            "num_rows": len(df),
            "df": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
        deps=[stage_teams],
        group_name="update_annual",
        config_schema={"season": int, "ids": [int]}
)
def rosters_files(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    download the team rosters for a specific season
    if ids are specified in config then only use those, otherwise hit stage_teams table
    """
    invalid_urls = []
    valid_urls = []
    ids = context.op_config['ids']
    if not ids:
        with database.get_connection() as conn:
            df = conn.execute("select id from stage_teams;").df()
        ids = df['id']

    for id in ids:
        context.log.info(f'team id: {id}')
        roster_url = ROSTER_URL(id, context.op_config['season'])
        context.log.info(roster_url)
        try:
            roster = fetch_data(roster_url, context)
            path = os.path.join(storage.filepath, "rosters", str(id))
            os.makedirs(path, exist_ok=True)
            with open(os.path.join(path, "roster.json"), "w") as f:
                context.log.info(f"""
                                id: {id} team: {roster['team']['displayName']} players: {len(roster['team']['athletes'])}
                                """)
                json.dump(roster, f)
            valid_urls.append(roster_url)
        except ValueError as e:
            context.log.info(e)
            invalid_urls.append(roster_url)

    return MaterializeResult(
        metadata={
            "valid_urls": MetadataValue.md(pandas.DataFrame(valid_urls).to_markdown()),
            "invalid_urls": MetadataValue.md(pandas.DataFrame(invalid_urls).to_markdown())
        }
    )

@asset(
    group_name="update_daily",
    partitions_def=daily_partition,
    retry_policy=RetryPolicy(
            max_retries=5,        # Maximum number of retries
            delay=15              # Delay between retries in seconds
        ),
    check_specs=[AssetCheckSpec(name="has_at_least_one_game", asset="daily_scoreboard_file"),
                AssetCheckSpec(name="all_games_completed_is_true", asset="daily_scoreboard_file"),
                 AssetCheckSpec(name="all_game_ids_have_nine_digits", asset="daily_scoreboard_file")]
)
def daily_scoreboard_file(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> Generator:
    """
    downloads scoreboard of games partitioned by day and writes them to disk
    """
    partition_date_str = context.partition_key
    date_obj = datetime.strptime(partition_date_str, "%Y-%m-%d")
    date_str = date_obj.strftime(DATE_FORMAT)
    scoreboard_url_complete = SCOREBOARD_URL_TEMPLATE(date_str)
    context.log.info(f'scoreboard_url: {scoreboard_url_complete}')
    scoreboard_info = fetch_data(scoreboard_url_complete, context)
    path = os.path.join(storage.filepath, "scoreboard", partition_date_str)
    os.makedirs(path, exist_ok=True)
    scoreboard_file_path = os.path.join(path,"scoreboard.json")
    context.log.info(f'scoreboard_file_path: {scoreboard_file_path}')

    with open(scoreboard_file_path, "w") as f:
        json.dump(scoreboard_info, f)

    scoreboard_metadata = queries.scoreboard_metadata(scoreboard_file_path)
    context.log.info(scoreboard_metadata)

    with database.get_connection() as conn:
        df = conn.execute(scoreboard_metadata).df()
        context.log.info(df.head())

    has_at_least_one_game = bool(len(df['id']) > 0)
    all_games_completed = not any(~df['completed'])
    valid_ids = bool(df['id'].apply(lambda x: bool(re.match(r'^4\d{8}$', x))).all())

    yield AssetCheckResult(
                passed=has_at_least_one_game,
                check_name="has_at_least_one_game"
    )

    if not has_at_least_one_game:
        raise Exception("Empty schedule. Aborting further materialization.")
    
    yield AssetCheckResult(
                passed=all_games_completed,
                check_name="all_games_completed_is_true"
            )
    
    yield AssetCheckResult(
                passed=valid_ids,
                check_name="all_game_ids_have_nine_digits"
            )
    
    yield Output(
        value=df,
        tags={
            "owner": "evan"
        },
        metadata={
            "num_games": len(df['id']),
            "any_overtime": MetadataValue.bool(any(df['period']>2)),
            "any_postseason": MetadataValue.bool(any(df['slug']=="post-season")),
            "schedule": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    retry_policy=RetryPolicy(
            max_retries=5,        # Maximum number of retries
            delay=15              # Delay between retries in seconds
        ),
    group_name="update_daily",
    partitions_def=daily_partition
)
def game_summary_files(context: AssetExecutionContext, storage: LocalFileStorage, daily_scoreboard_file: pandas.DataFrame) -> MaterializeResult:
    """
    fetches json data from espn game summary api 
    """

    partition_date = context.partition_key

    ids = daily_scoreboard_file['id']

    for id in ids:
        download_games_path = os.path.join(storage.filepath, "games", partition_date, id)
        os.makedirs(download_games_path, exist_ok=True)
        context.log.info(f'download_games_path = {download_games_path}')
        with open(os.path.join(download_games_path, "game.json"), "w") as f:
            game_summary_url_complete = GAME_SUMMARY_URL_TEMPLATE(id)
            game_summary = fetch_data(game_summary_url_complete, context)
            json.dump(game_summary, f)

    return MaterializeResult(
        metadata={
            "num_games": len(ids),
            "ids": MetadataValue.md(ids.to_markdown())
        }
    )

@asset(
        group_name="udpate_annual",
        deps=[rosters_files]
)
def stage_players(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage player info from rosters download
    """
    with database.get_connection() as conn:
        conn.execute("drop table if exists stage_players;")
        conn.execute(queries.create_table_stage_players())

    all_rosters_paths = glob(os.path.join(storage.filepath, "rosters", "*", "roster.json"))
    with database.get_connection() as conn:
        context.log.info(queries.insert_table_stage_players(all_rosters_paths))
        df = conn.execute(queries.insert_table_stage_players(all_rosters_paths)).df()

    return MaterializeResult(
        metadata={
            "players": MetadataValue.md(df.sample(n=100).to_markdown()),
            "num_players": len(df['id'])
        }
    )

@asset(
    deps=[daily_scoreboard_file],
    group_name="update_daily",
    partitions_def=daily_partition,
)
def stage_daily_scoreboard(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    loads scoreboard of events from downloaded files into duckdb | partitioned by day
    """

    partition_date = context.partition_key
    path = os.path.join(storage.filepath, "scoreboard", partition_date, "scoreboard.json")
    
    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            res = conn.execute(f"select length(events) as num_events from read_json('{path}');").fetchnumpy()
            if not res['num_events'] > 0:
                raise Exception("No games scheduled. Abort further materialization steps.")
            conn.execute(queries.create_table_stage_daily_scoreboard())
            context.log.info(queries.insert_table_stage_daily_scoreboard(path, partition_date))
            res = conn.execute(queries.insert_table_stage_daily_scoreboard(path, partition_date)).fetchnumpy()
            df = conn.execute(f"from stage_daily_scoreboard where date='{partition_date}';").df()

    return MaterializeResult(
        metadata={
            "num_games_inserted": len(res['game_id']),
            "summary": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=[game_summary_files, stage_daily_scoreboard],
    group_name="update_daily",
    partitions_def=daily_partition,
)
def stage_game_logs(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    staging table for game logs containing box score totals for each team
    """

    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            context.log.info(queries.create_table_stage_game_logs())
            conn.execute(queries.create_table_stage_game_logs())
            context.log.info(queries.insert_table_stage_game_logs(files))
            res = conn.execute(queries.insert_table_stage_game_logs(files)).fetchnumpy()
            df = conn.execute(f"from stage_game_logs where date='{partition_date}';").df()

    return MaterializeResult(
        metadata={
            "num_games_inserted": len(res['game_id']),
            "num_game_files": MetadataValue.int(len(files)),
            "summary": MetadataValue.md(df[['game_id', 'date','team_1_location','team_2_location']].to_markdown())
        }
    )

@asset(
    deps=[game_summary_files],
    group_name="update_daily",
    partitions_def=daily_partition,
)
def stage_player_lines(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage table for player lines (ie box score stats) for each game
    """

    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            conn.execute(queries.create_table_stage_player_lines())
            context.log.info(queries.insert_table_stage_player_lines(files, partition_date))
            res = conn.execute(queries.insert_table_stage_player_lines(files, partition_date)).fetchnumpy()
            df = conn.execute(f"from stage_player_lines where date='{partition_date}';").df()
    
    return MaterializeResult(
        metadata={
            "num_lines_inserted": len(res['player_id']),
            "num_game_files": MetadataValue.int(len(files)),
            "summary": MetadataValue.md(df[['game_id','date','player_id','name','stats']].to_markdown())
        }
    )

@asset(
    deps=[game_summary_files],
    group_name="update_daily",
    partitions_def=daily_partition,
)
def stage_plays(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    insert plays into staging table
    """
    partition_date = context.partition_key
    files = glob(os.path.join(storage.filepath, "games", partition_date, '*', 'game.json'))

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            conn.execute(queries.create_table_stage_plays())
            context.log.info(queries.insert_table_stage_plays(files, partition_date))
            res = conn.execute(queries.insert_table_stage_plays(files, partition_date)).fetchnumpy()
            df = conn.execute(f"from stage_plays where date='{partition_date}' using sample reservoir(5%);").df()
    
    return MaterializeResult(
        metadata={
            "num_plays_inserted": len(res['play_id']),
            "num_game_files": MetadataValue.int(len(files)),
            "summary": MetadataValue.md(df[['game_id','date','play_index','text','assist','period','game_clock']].to_markdown())
        }
    )

@asset(
    group_name="update_annual"
)
def stage_rsci_rankings(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    stage rsci rankings from csv files
    """
    path = os.path.join(storage.filepath, "rsci", "*.csv")
    context.log.info(path)

    with database.get_connection() as conn:
        conn.execute("drop table if exists stage_rsci_rankings;")
        conn.execute(f"""
                     create table stage_rsci_rankings as (
                     from read_csv('{path}', header=true, filename=true)
                     );
                     """)
        schema = conn.execute("describe from stage_rsci_rankings limit 1;").df()
        df = conn.execute("from stage_rsci_rankings;").df()
        context.log.info(schema)
    
    return MaterializeResult(
        metadata={
            "schema": MetadataValue.md(schema.to_markdown()),
            "count": MetadataValue.int(len(df)),
            "sample": MetadataValue.md(df.sample(n=50).to_markdown())
        }
    )

@asset(
    deps=[stage_plays],
    group_name="update_daily",
    partitions_def=daily_partition,
)
def stage_player_shots_by_game(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    partition_date = context.partition_key

    with database.get_connection() as conn:
        context.log.info(queries.create_table_stage_player_shots_by_game())
        conn.execute(queries.create_table_stage_player_shots_by_game())
        context.log.info(queries.insert_table_stage_player_shots_by_game(partition_date))
        res = conn.execute(queries.insert_table_stage_player_shots_by_game(partition_date)).fetchnumpy()
        df = conn.execute(f"from stage_player_shots_by_game where date='{partition_date}' limit 50;").df()

    return MaterializeResult(
        metadata={
            "num_players": MetadataValue.int(len(res['player_id'])),
            "sample": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=[stage_plays],
    group_name="update_daily",
    partitions_def=daily_partition,
)
def stage_player_assists_by_game(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    date = context.partition_key
    sql_query = queries.stage_player_assists_by_game(date)
    context.log.info(sql_query)

    with database.get_connection() as conn:
        conn.execute(sql_query)
        count_players = conn.execute("select count(distinct player_id) as players from stage_player_assists_by_game;").fetchone()[0]
        df = conn.execute("from stage_player_assists_by_game limit 10;").df()
        context.log.info(count_players)

    return MaterializeResult(
        metadata={
            "players": MetadataValue.int(count_players),
            "sample": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=[stage_player_shots_by_game]
)
def html_table_from_stage_player_shots(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    with database.get_connection() as conn:
        df = conn.execute("from stage_player_shots_by_game limit 100;").df()
    
    html = df.to_html()
    path = os.path.join(storage.filepath, "html")
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "daily.html"), "w") as f:
        f.write(html)

    return MaterializeResult(
        metadata={
            "count": len(df)
        }
    )

@asset(
    deps=[
        stage_player_lines, 
        stage_player_shots_by_game, 
        stage_players, 
        stage_game_logs
    ],
    partitions_def=daily_partition,
    group_name="update_daily"
)
def stage_top_lines(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    """
    stage table that will be turned into html report
    """
    start_date = os.environ['START_DATE']
    end_date = os.environ['END_DATE']

    sql_query = queries.insert_table_stage_top_lines(start_date=start_date, end_date=end_date)
    context.log.info(sql_query)

    with database.get_connection() as conn:
        conn.execute(sql_query)

    return MaterializeResult(
        metadata={
            "query": MetadataValue.md(sql_query)
        }
    )

@asset(
    deps=[stage_top_lines],
    config_schema={
        "start_date": str,
        "end_date": str
    }
)
def top_lines_html_report(context: AssetExecutionContext, database: DuckDBResource, storage: LocalFileStorage) -> MaterializeResult:
    """
    create html report for newsletter
    """
    start_date = context.op_config['start_date']
    end_date = context.op_config['end_date']

    with database.get_connection() as conn:
        df = conn.execute(f"""
            select
                name,
                date
            from
            stage_top_lines
            where date between '{start_date}' and '{end_date}';
        """).df()

    html = df.to_html()
    path = os.path.join(storage.filepath, "top_lines", end_date)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "report.html"), "w") as f:
        f.write(html)
    
    return MaterializeResult(
        metadata={
            "df": MetadataValue.md(df.to_markdown())
        }
    )

@asset(
    deps=[stage_game_logs]
)
def team_net_rating_model(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    """
    use catboost model to do regression on game results
    """

    from sklearn.preprocessing import OneHotEncoder
    from sklearn.linear_model import ElasticNet, Ridge

    with database.get_connection() as conn:
        df = conn.execute("""
            select
                game_id,
                poss,
                minutes,
                team_1_id,
                team_2_id,
                team_1_pts,
                team_2_pts
            from stage_game_logs
            where (team_2_pts-team_1_pts) is not null;
    """).df()
    
    # Features (team IDs) and target (point differential)
    categories = sorted(list(set(df.team_1_id.values.tolist()+df.team_2_id.values.tolist())))
    ohe = OneHotEncoder(categories=[categories], sparse_output=True)
    all_teams = pandas.concat([df['team_1_id'], df['team_2_id']])
    ohe.fit(all_teams.values.reshape(-1, 1))
    encoded_team_1 = ohe.transform(df[['team_1_id']].values)
    encoded_team_2 = ohe.transform(df[['team_2_id']].values)
    encoded = (-1*encoded_team_1) + encoded_team_2
    feature_names = ohe.get_feature_names_out()
    team_ids = [int(name[3:]) for name in feature_names]

    X = encoded  # Using team IDs as features
    y = df['team_2_pts']-df['team_1_pts']  # Target: point differential

    context.log.info(f"any nan? {y.isna().any()}")

    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Initialize the regressor
    model = Ridge(alpha=0.1, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    # context.log.info(rmse)
    # context.log.info(type(rmse))
    coefficients = model.coef_
    # context.log.info(coefficients)
    scores = list(zip(team_ids, coefficients))
    sorted_team_scores = sorted(scores, key=lambda x: x[1], reverse=True)
    # context.log.info(sorted_team_scores)
    rankings_df = pandas.DataFrame.from_records(sorted_team_scores, columns=['id', 'score'])
    context.log.info(rankings_df.head())

    with database.get_connection() as conn:
        conn.register("df", rankings_df)
        query = """
            SELECT 
                df.id, 
                displayName,
                shortConferenceName,
                score
            FROM df
            JOIN stage_teams st ON df.id = st.id
        """
        result_df = conn.execute(query).df()

    return MaterializeResult(
        metadata={
            "rmse": MetadataValue.float(float(rmse)),
            "rankings": MetadataValue.md(result_df.to_markdown()),
            "num_teams": MetadataValue.int(len(feature_names.tolist()))
        }
    )

@asset(
    deps=[stage_game_logs]
)
def team_split_rating_model(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    """
    use catboost model to do regression on game results
    """

    with database.get_connection() as conn:
        df = conn.execute("""
            with combined as 
            (select
                game_id,
                poss,
                minutes,
                0.0 as home,
                'o' || team_1_id as offense,
                'd' || team_2_id as defense,
                100.0*team_1_pts/poss as rating
            from stage_game_logs
            where team_1_pts is not null
            union all
            select
                game_id,
                poss,
                minutes,
                1.0 as home,
                'o' || team_2_id as offense,
                'd' || team_1_id as defense,
                100.0*team_2_pts/poss as rating
            from stage_game_logs
            where team_2_pts is not null)
            select
                *
            from combined
            order by random();
    """).df()
    
    # Features (team IDs) and target (point differential)
    ohe_offense = OneHotEncoder(sparse_output=False)
    ohe_defense = OneHotEncoder(sparse_output=False)
    encoded_offense = ohe_offense.fit_transform(df[['offense']].values)
    encoded_defense = ohe_defense.fit_transform(df[['defense']].values)
    features_offense = ohe_offense.get_feature_names_out()
    features_defense = ohe_defense.get_feature_names_out()
    team_ids_offense = [name[4:] for name in features_offense]
    team_ids_defense = [name[4:] for name in features_defense]
    context.log.info(df.home)
    X = pandas.concat([pandas.DataFrame(df.home.values), pandas.DataFrame(encoded_offense), pandas.DataFrame(encoded_defense)], axis=1)  
    # context.log.info(X)
    y = df['rating']  # Target: point differential

    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=42)
    context.log.info(X_train)
    # Initialize the regressor
    alphas = numpy.array([0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0])
    model = RidgeCV(alphas=alphas, cv=5)
    model.fit(X_train, y_train)
    best_alpha = model.alpha_
    y_pred = model.predict(X_test)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    coefficients = model.coef_
    bias = model.intercept_
    hca = coefficients[0]
    oratings = list(zip(team_ids_offense, bias+coefficients[1:len(team_ids_offense)]))
    dratings = list(zip(team_ids_defense, bias+coefficients[1+len(team_ids_defense):]))
    ortg_df = pandas.DataFrame.from_records(oratings, columns=['id', 'rating'])
    drtg_df = pandas.DataFrame.from_records(dratings, columns=['id', 'rating'])

    with database.get_connection() as conn:
        conn.register("ortg_df", ortg_df)
        conn.register("drtg_df", drtg_df)
        query = """
            SELECT 
                st.id, 
                displayName,
                shortConferenceName,
                o.rating-d.rating as net,
                o.rating as ortg,
                exp(2 * ((o.rating / avg(o.rating) over ()) - 1)) as ortgn,
                d.rating as drtg,
                exp(2 * ((d.rating / avg(d.rating) over ()) - 1)) as drtgn
            FROM ortg_df o
            JOIN stage_teams st ON o.id=st.id
            JOIN drtg_df d on d.id=st.id
        """
        result_df = conn.execute(query).df()

    return MaterializeResult(
        metadata={
            "rmse": MetadataValue.float(float(rmse)),
            "bias": MetadataValue.float(float(bias)),
            "alpha": MetadataValue.float(float(best_alpha)),
            "hca": MetadataValue.float(float(hca)),
            "num_teams_offense": MetadataValue.int(len(features_offense.tolist())),
            "top25": MetadataValue.md(result_df.sort_values(by='net', ascending=False).head(25).to_markdown()),
            "top25_off": MetadataValue.md(result_df.sort_values(by='ortg', ascending=False).head(25).to_markdown()),
            "top25_def": MetadataValue.md(result_df.sort_values(by='drtg', ascending=True).head(25).to_markdown()),
        }
    )

def build_stage_box_stat_adjustment_factors(stat: Literal["tov", "fta", "ftm", "fga", "fg3a", "fg3m", 
                                                          "orb", "ast", "stl", "blk", "drb"],
                                       higher_is_better: bool=True,
                                       offense: bool=True,
                                       alphas: list[float]=[0.1, 1.0, 10.0]) -> AssetsDefinition:
    """
    factory function for building models for a stat in stage_game_logs
    """
    @asset(
        deps=[stage_game_logs],
        name=f"adj_{stat}_per_100_model",
        group_name="box_stat_models",
        description=f"model for predicting rate of {stat} per 100 possessions between two teams"
    )
    def _asset(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
        """
        use catboost model to do regression on game results
        """
        team_1_or_2 = lambda offense: 1 if offense else 2

        with fasteners.InterProcessLock('/tmp/duckdb.lock'):
            with database.get_connection() as conn:
                df = conn.execute(f"""
                    with combined as 
                    (select
                        game_id,
                        poss,
                        0.0 as home,
                        'o' || team_1_id as offense,
                        'd' || team_2_id as defense,
                        100.0*team_{team_1_or_2(offense)}_stats.{stat}/poss as rating
                    from stage_game_logs
                    where team_1_pts is not null
                    union all
                    select
                        game_id,
                        poss,
                        1.0 as home,
                        'o' || team_2_id as offense,
                        'd' || team_1_id as defense,
                        100.0*team_{team_1_or_2(not offense)}_stats.{stat}/poss as rating
                    from stage_game_logs
                    where team_2_pts is not null)
                    select
                        *
                    from combined
            """).df()
        
        # Features (team IDs) and target (point differential)
        ohe_offense = OneHotEncoder(sparse_output=False)
        ohe_defense = OneHotEncoder(sparse_output=False)
        encoded_offense = ohe_offense.fit_transform(df[['offense']].values)
        encoded_defense = ohe_defense.fit_transform(df[['defense']].values)
        features_offense = ohe_offense.get_feature_names_out()
        features_defense = ohe_defense.get_feature_names_out()
        team_ids_offense = [name[4:] for name in features_offense]
        team_ids_defense = [name[4:] for name in features_defense]
        context.log.info(df.home)
        X = pandas.concat([pandas.DataFrame(df.home.values), pandas.DataFrame(encoded_offense), pandas.DataFrame(encoded_defense)], axis=1)  
        y = df['rating']  # Target: point differential

        # Split data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=42)
        context.log.info(X_train)
        # Initialize the regressor
        model = RidgeCV(alphas=numpy.array(alphas), cv=5)
        model.fit(X_train, y_train)
        best_alpha = model.alpha_
        y_pred = model.predict(X_test)
        rmse = mean_squared_error(y_test, y_pred, squared=False)
        coefficients = model.coef_
        bias = model.intercept_
        hca = coefficients[0]
        oratings = list(zip(team_ids_offense, bias+coefficients[1:len(team_ids_offense)]))
        dratings = list(zip(team_ids_defense, bias+coefficients[1+len(team_ids_defense):]))
        ortg_df = pandas.DataFrame.from_records(oratings, columns=['id', 'rating'])
        drtg_df = pandas.DataFrame.from_records(dratings, columns=['id', 'rating'])

        with fasteners.InterProcessLock('/tmp/duckdb.lock'):
            with database.get_connection() as conn:
                conn.register("ortg_df", ortg_df)
                conn.register("drtg_df", drtg_df)
                conn.execute("""
                CREATE TABLE IF NOT EXISTS stage_box_stat_adjustment_factors (
                    team_id INT,
                    stat VARCHAR,
                    ortg DOUBLE,
                    drtg DOUBLE,
                    PRIMARY KEY (team_id, stat)
                    );
                """)

                query = f"""
                    INSERT OR IGNORE INTO stage_box_stat_adjustment_factors 
                    (
                    SELECT 
                        st.id as team_id, 
                        '{stat}' as stat,
                        o.rating / avg(o.rating) over () as ortg,
                        d.rating / avg(d.rating) over () as drtg
                    FROM ortg_df o
                    JOIN stage_teams st ON o.id=st.id
                    JOIN drtg_df d on d.id=st.id
                    )
                    returning team_id, stat, ortg, drtg;
                """
                result_df = conn.execute(query).df()

        return MaterializeResult(
            metadata={
                "rmse": MetadataValue.float(float(rmse)),
                "bias": MetadataValue.float(float(bias)),
                "alpha": MetadataValue.float(float(best_alpha)),
                "hca": MetadataValue.float(float(hca)),
                "num_teams_offense": MetadataValue.int(len(features_offense.tolist())),
                "top_off": MetadataValue.md(result_df.sort_values(by='ortg', ascending=not higher_is_better).head(25).to_markdown()),
                "top_def": MetadataValue.md(result_df.sort_values(by='drtg', ascending=higher_is_better).head(25).to_markdown()),
            }
        )
    
    return _asset

alphas = [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0]
tov_model = build_stage_box_stat_adjustment_factors(stat="tov", higher_is_better=False, alphas=alphas)
ast_model = build_stage_box_stat_adjustment_factors(stat="ast", alphas=alphas)
fta_model = build_stage_box_stat_adjustment_factors(stat="fta", alphas=alphas)
ftm_model = build_stage_box_stat_adjustment_factors(stat="ftm", alphas=alphas)
fga_model = build_stage_box_stat_adjustment_factors(stat="fga", alphas=alphas)
fg3a_model = build_stage_box_stat_adjustment_factors(stat="fg3a", alphas=alphas)
fg3m_model = build_stage_box_stat_adjustment_factors(stat="fg3m", alphas=alphas)
orb_model = build_stage_box_stat_adjustment_factors(stat="orb", alphas=alphas)
drb_model = build_stage_box_stat_adjustment_factors(stat="drb", offense=False, alphas=alphas)
stl_model = build_stage_box_stat_adjustment_factors(stat="stl", offense=False, alphas=alphas)
blk_model = build_stage_box_stat_adjustment_factors(stat="blk", offense=False, alphas=alphas)

def build_stage_shot_type_adjustment_factors(shot: Literal["ast_dunk", "unast_dunk", "miss_dunk", 
                                                           "ast_layup", "unast_layup", "miss_layup", 
                                                           "ast_mid", "unast_mid", "miss_mid",
                                                           "ast_3pt", "unast_3pt", "miss_3pt",
                                                           "ast_tip", "unast_tip", "miss_tip"],
                                       higher_is_better: bool=True,
                                       alphas: list[float]=[0.1, 1.0, 10.0]) -> AssetsDefinition:
    """
    factory function for building models for a shot type in stage_player_shots_by_game
    """
    @asset(
        deps=[stage_game_logs, stage_player_shots_by_game],
        name=f"adj_{shot}_per_100_model",
        group_name="shot_type_models",
        description=f"model for predicting rate of {shot} per 100 possessions between two teams"
    )
    def _asset(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
        """
        use catboost model to do regression on game results
        """

        with fasteners.InterProcessLock('/tmp/duckdb.lock'):
            with database.get_connection() as conn:
                df = conn.execute(f"""
                    with shots as (
                        select
                            game_id,
                            'o' || team_id as offense,
                            'd' || opp_id as defense,
                            home,
                            sum({shot}) as total
                        from stage_player_shots_by_game
                        group by ALL
                    ),
                    games as (
                        select
                            game_id as gid,
                            poss
                        from stage_game_logs
                        where poss > 0
                    )
                    select
                        game_id,
                        poss,
                        case when home is true then 1.0 else 0.0 end as home,
                        offense,
                        defense,
                        100.0*total/poss as rating
                    from shots s join games g on
                    s.game_id=g.gid
            """).df()
        
        # Features (team IDs) and target (point differential)
        ohe_offense = OneHotEncoder(sparse_output=False)
        ohe_defense = OneHotEncoder(sparse_output=False)
        encoded_offense = ohe_offense.fit_transform(df[['offense']].values)
        encoded_defense = ohe_defense.fit_transform(df[['defense']].values)
        features_offense = ohe_offense.get_feature_names_out()
        features_defense = ohe_defense.get_feature_names_out()
        team_ids_offense = [name[4:] for name in features_offense]
        team_ids_defense = [name[4:] for name in features_defense]
        context.log.info(df.home)
        X = pandas.concat([pandas.DataFrame(df.home.values), pandas.DataFrame(encoded_offense), pandas.DataFrame(encoded_defense)], axis=1)  
        y = df['rating']  # Target: point differential

        # Split data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.05, random_state=42)
        context.log.info(X_train)
        # Initialize the regressor
        model = RidgeCV(alphas=numpy.array(alphas), cv=5)
        model.fit(X_train, y_train)
        best_alpha = model.alpha_
        y_pred = model.predict(X_test)
        rmse = mean_squared_error(y_test, y_pred, squared=False)
        coefficients = model.coef_
        bias = model.intercept_
        hca = coefficients[0]
        oratings = list(zip(team_ids_offense, bias+coefficients[1:len(team_ids_offense)]))
        dratings = list(zip(team_ids_defense, bias+coefficients[1+len(team_ids_defense):]))
        ortg_df = pandas.DataFrame.from_records(oratings, columns=['id', 'rating'])
        drtg_df = pandas.DataFrame.from_records(dratings, columns=['id', 'rating'])

        with fasteners.InterProcessLock('/tmp/duckdb.lock'):
            with database.get_connection() as conn:
                conn.register("ortg_df", ortg_df)
                conn.register("drtg_df", drtg_df)
                conn.execute("""
                CREATE TABLE IF NOT EXISTS stage_shot_type_adjustment_factors (
                    team_id INT,
                    stat VARCHAR,
                    ortg DOUBLE,
                    drtg DOUBLE,
                    PRIMARY KEY (team_id, stat)
                    );
                """)

                query = f"""
                    INSERT OR IGNORE INTO stage_shot_type_adjustment_factors 
                    (
                    SELECT 
                        st.id as team_id, 
                        '{shot}' as stat,
                        o.rating / avg(o.rating) over () as ortg,
                        d.rating / avg(d.rating) over () as drtg
                    FROM ortg_df o
                    JOIN stage_teams st ON o.id=st.id
                    JOIN drtg_df d on d.id=st.id
                    )
                    returning team_id, stat, ortg, drtg;
                """
                result_df = conn.execute(query).df()

        return MaterializeResult(
            metadata={
                "rmse": MetadataValue.float(float(rmse)),
                "bias": MetadataValue.float(float(bias)),
                "alpha": MetadataValue.float(float(best_alpha)),
                "hca": MetadataValue.float(float(hca)),
                "num_teams_offense": MetadataValue.int(len(features_offense.tolist())),
                "top_off": MetadataValue.md(result_df.sort_values(by='ortg', ascending=not higher_is_better).head(25).to_markdown()),
                "top_def": MetadataValue.md(result_df.sort_values(by='drtg', ascending=higher_is_better).head(25).to_markdown()),
            }
        )
    
    return _asset

ast_3pt = build_stage_shot_type_adjustment_factors(shot="ast_3pt", alphas=alphas)
unast_3pt = build_stage_shot_type_adjustment_factors(shot="unast_3pt", alphas=alphas)
miss_3pt = build_stage_shot_type_adjustment_factors(shot="miss_3pt", higher_is_better=False, alphas=alphas)
ast_dunk = build_stage_shot_type_adjustment_factors(shot="ast_dunk", alphas=alphas)
unast_dunk = build_stage_shot_type_adjustment_factors(shot="unast_dunk", alphas=alphas)
miss_dunk = build_stage_shot_type_adjustment_factors(shot="miss_dunk", higher_is_better=False, alphas=alphas)
ast_layup = build_stage_shot_type_adjustment_factors(shot="ast_layup", alphas=alphas)
unast_layup = build_stage_shot_type_adjustment_factors(shot="unast_layup", alphas=alphas)
miss_layup = build_stage_shot_type_adjustment_factors(shot="miss_layup", higher_is_better=False, alphas=alphas)
ast_mid = build_stage_shot_type_adjustment_factors(shot="ast_mid", alphas=alphas)
unast_mid = build_stage_shot_type_adjustment_factors(shot="unast_mid", alphas=alphas)
miss_mid = build_stage_shot_type_adjustment_factors(shot="miss_mid", higher_is_better=False, alphas=alphas)
ast_tip = build_stage_shot_type_adjustment_factors(shot="ast_tip", alphas=alphas)
unast_tip = build_stage_shot_type_adjustment_factors(shot="unast_tip", alphas=alphas)
miss_tip = build_stage_shot_type_adjustment_factors(shot="miss_tip", higher_is_better=False, alphas=alphas)
