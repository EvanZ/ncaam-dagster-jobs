"""
Dagster assets for generating JSON data files for the Top Lines web app.
These assets query the same data as the HTML reports but output JSON
that can be consumed by the Vue.js frontend.
"""

import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Generator
from pathlib import Path
import boto3
from dotenv import load_dotenv
from datetime import date, timedelta

from dagster import (
    asset,
    Field,
    Array,
    Bool,
    String,
    Int,
    Noneable,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    AssetsDefinition,
)
from dagster_duckdb import DuckDBResource
import fasteners
import pandas as pd

from . import queries
from .constants import (
    PYTHON,
    DUCKDB,
    WEB_EXPORT,
    SEASON_START_DATE,
    DATE_FORMAT,
    SCOREBOARD_URL_TEMPLATE,
    SCOREBOARD_URL_TEMPLATE_WOMEN,
)
from ..partitions import daily_partition
from ..resources import LocalFileStorage
from ..utils.utils import fetch_data

# Load environment variables from dagster-jobs/.env if present
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

def convert_structs_to_dicts(df: pd.DataFrame) -> list[dict]:
    """
    Convert pandas DataFrame with DuckDB structs to JSON-serializable dicts.
    DuckDB struct columns come through as dicts, but we need to ensure
    all values are JSON serializable.
    """
    records = df.to_dict(orient="records")
    
    def make_serializable(obj):
        if pd.isna(obj):
            return None
        if isinstance(obj, dict):
            return {k: make_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [make_serializable(item) for item in obj]
        if hasattr(obj, 'isoformat'):  # datetime-like
            return obj.isoformat()
        if hasattr(obj, 'item'):  # numpy types
            return obj.item()
        return obj
    
    return [make_serializable(record) for record in records]


def resolve_women(context: AssetExecutionContext) -> bool:
    women = context.op_config.get("women", False)
    if women:
        return True
    return context.job_name == "web_export_women"

def default_end_date() -> str:
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

def load_ez_history(
    conn,
    player_ids: list[int],
    games: int,
    women: bool,
) -> dict[int, list[float]]:
    if not player_ids or games <= 0:
        return {}
    ids = []
    for pid in player_ids:
        try:
            ids.append(int(pid))
        except (TypeError, ValueError):
            continue
    if not ids:
        return {}
    table = "stage_top_lines_women" if women else "stage_top_lines"
    query = f"""
    with recent as (
        select
            player_id,
            ez,
            date,
            game_id,
            row_number() over (
                partition by player_id
                order by date desc, game_id desc
            ) as rn
        from {table}
        where player_id in ({', '.join(map(str, ids))})
    )
    select
        player_id,
        list(ez order by date asc, game_id asc) as ez_history
    from recent
    where rn <= {games}
    group by player_id
    """
    df = conn.execute(query).df()
    history = {}
    for row in df.itertuples(index=False):
        ez_history = row.ez_history
        if ez_history is None:
            history[int(row.player_id)] = []
        else:
            history[int(row.player_id)] = list(ez_history)
    return history


def s3_config():
    env = os.environ.get("ENVIRONMENT", "DEV").upper()
    bucket = os.environ.get("S3_BUCKET")
    prefix = os.environ.get("S3_WEB_PREFIX", "data/web/")
    prefix = prefix.rstrip("/") + "/"
    votes_prefix = os.environ.get("S3_VOTES_PREFIX", "votes/")
    votes_prefix = votes_prefix.rstrip("/") + "/"
    return env, bucket, prefix, votes_prefix


def maybe_upload_json(local_path: str, key: str):
    env, bucket, prefix, _ = s3_config()
    if env != "PROD" or not bucket:
        return
    s3_key = prefix + key.lstrip("/")
    boto3.client("s3").upload_file(
        Filename=local_path,
        Bucket=bucket,
        Key=s3_key,
        ExtraArgs={"ContentType": "application/json"},
    )


def load_elo_ratings(gender: str) -> dict[int, dict]:
    """
    Load Elo ratings for a gender and return a mapping of player_id to
    {"elo_rating": float, "elo_rank": int}. Safe to call when file is missing.
    """
    env, bucket, prefix, _ = s3_config()
    # Try S3 in PROD
    if env == "PROD" and bucket:
        try:
            key = f"{prefix}{gender}/elo_rankings.json"
            obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
            data = json.loads(obj["Body"].read())
            players = data.get("players", [])
        except Exception:
            players = []
    else:
        base = Path(os.environ.get("DAGSTER_HOME", ".")) / "data" / "web" / gender / "elo_rankings.json"
        if not base.exists():
            return {}
        try:
            data = json.loads(base.read_text())
            players = data.get("players", [])
        except Exception:
            players = []

    if not players:
        return {}

    sorted_players = sorted(players, key=lambda p: p.get("rating", 0), reverse=True)
    return {
        p.get("player_id"): {
            "elo_rating": p.get("rating"),
            "elo_rank": idx + 1,
        }
        for idx, p in enumerate(sorted_players)
        if p.get("player_id") is not None
    }


def _write_top_lines_by_date(
    partition_date: str,
    df: pd.DataFrame,
    gender: str,
    ez_history: dict[int, list[float]] | None = None,
) -> str:
    history = ez_history or {}
    players = convert_structs_to_dicts(df)
    for player in players:
        try:
            pid = int(player.get("player_id"))
        except (TypeError, ValueError):
            pid = None
        if pid is None:
            continue
        if pid in history:
            player["ez_history"] = history[pid]
    web_data_path = os.path.join(
        os.environ.get("DAGSTER_HOME", "."),
        "data",
        "web",
        gender,
        "toplines",
    )
    os.makedirs(web_data_path, exist_ok=True)

    output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "date": partition_date,
            "gender": gender,
            "total_players": len(players),
        },
        "players": players,
    }

    output_file = os.path.join(web_data_path, f"{partition_date}.json")
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2, default=str)
    maybe_upload_json(output_file, f"{gender}/toplines/{partition_date}.json")
    return output_file


@asset(
    partitions_def=daily_partition,
    config_schema={
        "top_n": Field(Int, default_value=500, is_required=False),
    },
    group_name=WEB_EXPORT,
    compute_kind=PYTHON,
)
def web_top_lines_by_date_men(
    context: AssetExecutionContext,
    database: DuckDBResource,
) -> MaterializeResult:
    partition_date = context.partition_key
    top_n = context.op_config.get("top_n") or 500
    sparkline_games = 10

    with database.get_connection() as conn:
        df = conn.execute(
            query=queries.top_lines_report_query(
                start_date=partition_date,
                end_date=partition_date,
                exp=[0, 1, 2, 3, 4, 5],
                top_n=top_n,
                women=False,
            )
        ).df()
        history_map = load_ez_history(conn, df["player_id"].tolist(), sparkline_games, women=False)

    output_file = _write_top_lines_by_date(partition_date, df, "men", history_map)
    return MaterializeResult(
        metadata={
            "output_file": MetadataValue.path(output_file),
            "date": MetadataValue.text(partition_date),
            "total_players": MetadataValue.int(len(df)),
        }
    )


@asset(
    partitions_def=daily_partition,
    config_schema={
        "top_n": Field(Int, default_value=500, is_required=False),
    },
    group_name=WEB_EXPORT,
    compute_kind=PYTHON,
)
def web_top_lines_by_date_women(
    context: AssetExecutionContext,
    database: DuckDBResource,
) -> MaterializeResult:
    partition_date = context.partition_key
    top_n = context.op_config.get("top_n") or 500
    sparkline_games = 10

    with database.get_connection() as conn:
        df = conn.execute(
            query=queries.top_lines_report_query(
                start_date=partition_date,
                end_date=partition_date,
                exp=[0, 1, 2, 3, 4, 5],
                top_n=top_n,
                women=True,
            )
        ).df()
        history_map = load_ez_history(conn, df["player_id"].tolist(), sparkline_games, women=True)

    output_file = _write_top_lines_by_date(partition_date, df, "women", history_map)
    return MaterializeResult(
        metadata={
            "output_file": MetadataValue.path(output_file),
            "date": MetadataValue.text(partition_date),
            "total_players": MetadataValue.int(len(df)),
        }
    )


@asset(
    deps=["stage_top_lines", "stage_players", "stage_teams", "stage_team_ratings"],
    config_schema={
        "end_date": Field(Noneable(String), default_value=None, is_required=False),
        "top_n": Field(Int, default_value=500, is_required=False),
        "sparkline_games": Field(Int, default_value=10, is_required=False),
        "women": Field(Bool, default_value=False, is_required=False)
    },
    group_name=WEB_EXPORT,
    compute_kind=PYTHON
)
def web_daily_report_json(
    context: AssetExecutionContext, 
    database: DuckDBResource, 
) -> MaterializeResult:
    """
    Generate JSON files for daily reports - top prospect performances.
    Generates 4 files for different date ranges: 1d, 2d, 3d, 7d.
    Output: data/web/daily/{end_date}_{days}d.json
    """
    from datetime import timedelta
    
    end_date = context.op_config.get('end_date') or default_end_date()
    top_n = context.op_config['top_n']
    sparkline_games = context.op_config.get("sparkline_games", 10)
    women = resolve_women(context)
    
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Date ranges to generate: 1, 2, 3, 7 days back
    date_ranges = [1, 2, 3, 7]
    elo_map = load_elo_ratings("women" if women else "men")
    
    web_data_path = os.path.join(
        os.environ.get('DAGSTER_HOME', '.'),
        "data", "web", "women" if women else "men", "daily"
    )
    os.makedirs(web_data_path, exist_ok=True)
    
    output_files = []
    total_players = 0
    
    for days in date_ranges:
        start_dt = end_dt - timedelta(days=days - 1)  # -1 because end_date is inclusive
        start_date = start_dt.strftime('%Y-%m-%d')
        
        context.log.info(f"Generating {days}d report: {start_date} to {end_date}")
        
        with database.get_connection() as conn:
            df = conn.execute(query=queries.top_lines_report_query(
                start_date=start_date,
                end_date=end_date,
                exp=[0, 1, 2, 3, 4, 5],  # All classes
                top_n=top_n,
                women=women
            )).df()

            # Convert to JSON-serializable format
            players = convert_structs_to_dicts(df)
            player_ids = [p.get("player_id") for p in players if p.get("player_id") is not None]
            history_map = load_ez_history(conn, player_ids, sparkline_games, women)
        # Attach Elo rating if available
        for player in players:
            pid = player.get("player_id")
            if pid in elo_map:
                player["elo_rating"] = elo_map[pid]["elo_rating"]
                player["elo_rank"] = elo_map[pid]["elo_rank"]
            player["ez_history"] = history_map.get(pid, [])
        
        output = {
            "meta": {
                "start_date": start_date,
                "end_date": end_date,
                "days": days,
                "generated_at": datetime.now().isoformat(),
                "total_players": len(players),
                "gender": "women" if women else "men",
                "sparkline_games": sparkline_games
            },
            "players": players
        }

        output_file = os.path.join(web_data_path, f"{end_date}_{days}d.json")
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2, default=str)
        maybe_upload_json(output_file, f"{'women' if women else 'men'}/daily/{end_date}_{days}d.json")
        
        context.log.info(f"Wrote {len(players)} players to {output_file}")
        output_files.append(output_file)
        total_players += len(players)

    return MaterializeResult(
        metadata={
            "output_files": MetadataValue.text(", ".join(output_files)),
            "total_reports": MetadataValue.int(len(date_ranges)),
            "date_ranges": MetadataValue.text("1d, 2d, 3d, 7d"),
            "end_date": MetadataValue.text(end_date)
        }
    )


@asset(
    deps=["stage_top_lines", "stage_players", "stage_teams", "stage_team_ratings", "stage_combine_measurements"],
    config_schema={
        "start_date": Field(String, default_value=SEASON_START_DATE, is_required=False),
        "end_date": Field(Noneable(String), default_value=None, is_required=False),
        "top_n": Field(Int, default_value=500, is_required=False),
        "include_player_ids": Field(Array(Int), default_value=[], is_required=False),
        "sparkline_games": Field(Int, default_value=10, is_required=False),
        "women": Field(Bool, default_value=False, is_required=False)
    },
    group_name=WEB_EXPORT,
    compute_kind=PYTHON
)
def web_season_rankings_json(
    context: AssetExecutionContext,
    database: DuckDBResource,
) -> MaterializeResult:
    """
    Generate JSON file for season rankings - cumulative prospect rankings.
    Output: data/web/rankings/{end_date}.json
    """
    start_date = context.op_config.get('start_date') or SEASON_START_DATE
    end_date = context.op_config.get('end_date') or default_end_date()
    top_n = context.op_config['top_n']
    include_player_ids = context.op_config.get('include_player_ids', [])
    sparkline_games = context.op_config.get("sparkline_games", 10)
    women = resolve_women(context)
    elo_map = load_elo_ratings("women" if women else "men")

    with database.get_connection() as conn:
        df = conn.execute(query=queries.prospect_rankings_report_query(
            start_date=start_date,
            end_date=end_date,
            exp=[0, 1, 2, 3, 4, 5],  # All classes
            top_n=top_n,
            women=women,
            include_player_ids=include_player_ids,
        )).df()
        player_ids = [pid for pid in df["player_id"].tolist() if pid is not None]
        history_map = load_ez_history(conn, player_ids, sparkline_games, women)

    # Convert to JSON-serializable format
    players = convert_structs_to_dicts(df)
    for player in players:
        pid = player.get("player_id")
        if pid in elo_map:
            player["elo_rating"] = elo_map[pid]["elo_rating"]
            player["elo_rank"] = elo_map[pid]["elo_rank"]
        player["ez_history"] = history_map.get(pid, [])

    output = {
        "meta": {
            "start_date": start_date,
            "end_date": end_date,
            "generated_at": datetime.now().isoformat(),
            "total_players": len(players),
            "gender": "women" if women else "men",
            "sparkline_games": sparkline_games
        },
        "players": players
    }

    # Write to web data folder
    web_data_path = os.path.join(
        os.environ.get('DAGSTER_HOME', '.'),
        "data", "web", "women" if women else "men", "rankings"
    )
    os.makedirs(web_data_path, exist_ok=True)
    
    output_file = os.path.join(web_data_path, f"{end_date}.json")
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2, default=str)
    maybe_upload_json(output_file, f"{'women' if women else 'men'}/rankings/{end_date}.json")
    
    context.log.info(f"Wrote {len(players)} players to {output_file}")

    return MaterializeResult(
        metadata={
            "output_file": MetadataValue.path(output_file),
            "num_players": MetadataValue.int(len(players)),
            "date_range": MetadataValue.text(f"{start_date} to {end_date}"),
            "sample": MetadataValue.md(df.head(5).to_markdown())
        }
    )


@asset(
    deps=["stage_players", "stage_teams", "stage_rsci_rankings"],
    config_schema={
        "women": Field(Bool, default_value=False, is_required=False)
    },
    group_name=WEB_EXPORT,
    compute_kind=DUCKDB
)
def web_prospects_json(
    context: AssetExecutionContext,
    database: DuckDBResource,
) -> MaterializeResult:
    """
    Generate JSON file with all prospects for the voting feature.
    Includes basic info needed for voting cards.
    Output: data/web/prospects.json
    """
    women = resolve_women(context)
    
    players_table = 'stage_players_women' if women else 'stage_players'
    teams_table = 'stage_teams_women' if women else 'stage_teams'
    rsci_table = 'stage_hoopgurlz_rankings' if women else 'stage_rsci_rankings'
    rsci_name_col = 'name' if women else 'Player'
    rsci_rank_col = 'rank' if women else 'RSCI'

    query = f"""
    SELECT
        p.id as player_id,
        p.full_name as name,
        p.jersey,
        p.headshot_href as headshot,
        p.display_height as height,
        p.display_weight as weight,
        p.experience_display_value as class,
        p.position_display_name as position,
        t.displayName as team,
        t.id as team_id,
        t.shortConferenceName as conference,
        p.city,
        p.state,
        p.country,
        COALESCE(r.{rsci_rank_col}, NULL) as rsci_rank,
        b.birthday,
        CASE WHEN b.birthday IS NOT NULL 
            THEN ROUND((DATE '2026-06-25' - b.birthday::DATE) / 365.25, 1)
            ELSE NULL 
        END as age_at_draft
    FROM {players_table} p
    JOIN {teams_table} t ON p.team_id = t.id
    LEFT JOIN {rsci_table} r ON p.full_name = r.{rsci_name_col}
    LEFT JOIN stage_prospect_birthdays b ON p.full_name = b.name
    WHERE p.experience_abbreviation IN ('Fr', 'So', 'Jr', 'Sr')
    ORDER BY COALESCE(r.{rsci_rank_col}, 9999), p.full_name
    """

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            df = conn.execute(query).df()

    players = convert_structs_to_dicts(df)

    output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "total_prospects": len(players),
            "gender": "women" if women else "men"
        },
        "prospects": players
    }

    # Write to web data folder
    web_data_path = os.path.join(
        os.environ.get('DAGSTER_HOME', '.'),
        "data", "web", "women" if women else "men"
    )
    os.makedirs(web_data_path, exist_ok=True)
    
    output_file = os.path.join(web_data_path, "prospects.json")
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2, default=str)
    maybe_upload_json(output_file, f"{'women' if women else 'men'}/prospects.json")
    
    context.log.info(f"Wrote {len(players)} prospects to {output_file}")

    return MaterializeResult(
        metadata={
            "output_file": MetadataValue.path(output_file),
            "num_prospects": MetadataValue.int(len(players)),
            "sample": MetadataValue.md(df.head(10).to_markdown())
        }
    )


@asset(
    deps=["stage_conferences"],
    config_schema={
        "women": Field(Bool, default_value=False, is_required=False)
    },
    group_name=WEB_EXPORT,
    compute_kind=DUCKDB
)
def web_conferences_json(
    context: AssetExecutionContext,
    database: DuckDBResource,
) -> MaterializeResult:
    """
    Generate JSON file with all conferences for filtering.
    Output: data/web/conferences.json
    """
    women = resolve_women(context)
    table = 'stage_conferences_women' if women else 'stage_conferences'

    query = f"""
    SELECT
        id as conference_id,
        name,
        shortName as short_name
    FROM {table}
    ORDER BY name
    """

    with fasteners.InterProcessLock('/tmp/duckdb.lock'):
        with database.get_connection() as conn:
            df = conn.execute(query).df()

    conferences = df.to_dict(orient="records")

    output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "total_conferences": len(conferences),
            "gender": "women" if women else "men"
        },
        "conferences": conferences
    }

    # Write to web data folder
    web_data_path = os.path.join(
        os.environ.get('DAGSTER_HOME', '.'),
        "data", "web", "women" if women else "men"
    )
    os.makedirs(web_data_path, exist_ok=True)
    
    output_file = os.path.join(web_data_path, "conferences.json")
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2, default=str)
    maybe_upload_json(output_file, f"{'women' if women else 'men'}/conferences.json")
    
    context.log.info(f"Wrote {len(conferences)} conferences to {output_file}")

    return MaterializeResult(
        metadata={
            "output_file": MetadataValue.path(output_file),
            "num_conferences": MetadataValue.int(len(conferences)),
            "conferences": MetadataValue.md(df.to_markdown())
        }
    )


@asset(
    deps=["stage_teams", "web_season_rankings_json", "web_conferences_json"],
    config_schema={
        "schedule_date": Field(Noneable(String), default_value=None, is_required=False),
        "rankings_date": Field(Noneable(String), default_value=None, is_required=False),
        "women": Field(Bool, default_value=False, is_required=False),
        "max_featured": Field(Noneable(Int), default_value=None, is_required=False),
        "max_per_team": Field(Noneable(Int), default_value=None, is_required=False),
        "days_ahead": Field(Int, default_value=0, is_required=False, description="Also export this many future days (inclusive of schedule_date)"),
        "days_back": Field(Int, default_value=2, is_required=False, description="Also export this many past days"),
    },
    group_name=WEB_EXPORT,
    compute_kind=PYTHON,
)
def web_schedule_json(
    context: AssetExecutionContext,
    database: DuckDBResource,
    storage: LocalFileStorage,
) -> MaterializeResult:
    """
    Build JSON for the current day's unplayed schedule with betting lines and featured prospects.
    Output: data/web/{gender}/schedule/{schedule_date}.json
    """

    cfg = context.op_config
    women = resolve_women(context)
    base_schedule_date = cfg.get("schedule_date") or date.today().isoformat()
    rankings_date = cfg.get("rankings_date")
    max_featured = cfg.get("max_featured")
    max_per_team = cfg.get("max_per_team")
    days_ahead = int(cfg.get("days_ahead") or 0)
    days_back = int(cfg.get("days_back") or 0)

    if days_ahead < 0:
        raise ValueError("days_ahead cannot be negative")
    if days_back < 0:
        raise ValueError("days_back cannot be negative")

    try:
        base_schedule_dt = datetime.strptime(base_schedule_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("schedule_date must be YYYY-MM-DD") from exc

    schedule_dates = [
        (base_schedule_dt + timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(-days_back, days_ahead + 1)
    ]

    def season_score(player: dict) -> float:
        try:
            ez = float(player.get("ez") or 0.0)
            gp = float(player.get("gp") or 0.0)
            return ez / gp if gp else 0.0
        except Exception:
            return 0.0

    def parse_record(records) -> tuple[int | None, int | None]:
        for rec in records or []:
            summary = rec.get("summary") or ""
            if "-" not in summary:
                continue
            try:
                wins, losses = summary.split("-")[:2]
                return int(wins.strip()), int(losses.strip())
            except Exception:
                continue
        return None, None

    def iso_dt(value: str | None):
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None

    def safe_float(value):
        try:
            return float(value)
        except Exception:
            return None

    def safe_int(value):
        try:
            return int(value)
        except Exception:
            return None

    teams_table = "stage_teams_women" if women else "stage_teams"
    with database.get_connection() as conn:
        teams_df = conn.execute(
            f"""
            select 
                id,
                displayName,
                location,
                abbreviation,
                shortConferenceName,
                conferenceName,
                color,
                alternateColor,
                logos
            from {teams_table}
            """
        ).df()
    teams_lookup = {}
    for row in teams_df.to_dict(orient="records"):
        try:
            tid = int(row.get("id"))
        except (TypeError, ValueError):
            continue
        raw_logos = row.get("logos")
        logo_list = []
        if isinstance(raw_logos, list):
            logo_list = raw_logos
        elif raw_logos is not None and hasattr(raw_logos, "tolist"):
            try:
                maybe_list = raw_logos.tolist()
                if isinstance(maybe_list, list):
                    logo_list = maybe_list
            except Exception:
                logo_list = []
        logo_href = None
        if logo_list and isinstance(logo_list[0], dict):
            logo_href = logo_list[0].get("href")
        teams_lookup[tid] = {
            "displayName": row.get("displayName"),
            "location": row.get("location"),
            "abbreviation": row.get("abbreviation"),
            "shortConferenceName": row.get("shortConferenceName"),
            "conferenceName": row.get("conferenceName"),
            "color": row.get("color"),
            "alternateColor": row.get("alternateColor"),
            "logo": logo_href,
        }

    web_root = os.path.join(os.environ.get("DAGSTER_HOME", "."), "data", "web")
    manifest_path = os.path.join(web_root, "manifest.json")
    if not rankings_date and os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r") as f:
                manifest = json.load(f)
            gender_key = "women" if women else "men"
            rankings_date = (manifest.get(gender_key, {}).get("rankings") or [manifest.get("latest_date")])[0]
        except Exception:
            rankings_date = None
    if not rankings_date:
        rankings_date = default_end_date()

    rankings_file = os.path.join(web_root, "women" if women else "men", "rankings", f"{rankings_date}.json")
    if not os.path.exists(rankings_file):
        raise FileNotFoundError(f"Rankings file not found: {rankings_file}")
    with open(rankings_file, "r") as f:
        rankings_data = json.load(f)

    power_confs = {"ACC", "Big Ten", "Big 12", "SEC", "Pac-12", "Big East"}

    players = rankings_data.get("players", [])
    ranked_players = sorted(players, key=season_score, reverse=True)
    team_players: dict[int, list[dict]] = defaultdict(list)
    for idx, player in enumerate(ranked_players, start=1):
        tid = player.get("team_id")
        if tid is None:
            continue
        try:
            tid = int(tid)
        except (TypeError, ValueError):
            continue
        team_players[tid].append({
            "player_id": player.get("player_id"),
            "team_id": tid,
            "overall_rank": idx,
            "display_rank": idx,
            "display_name": player.get("display_name"),
            "headshot_href": player.get("headshot_href"),
            "experience_display_value": player.get("experience_display_value"),
            "experience_abbreviation": player.get("experience_abbreviation"),
            "position_display_name": player.get("position_display_name"),
            "position": player.get("position"),
            "position_abbreviation": player.get("position_abbreviation"),
            "display_height": player.get("display_height"),
            "display_weight": player.get("display_weight"),
            "jersey": player.get("jersey"),
            "city": player.get("city"),
            "state": player.get("state"),
            "country": player.get("country"),
            "agency": player.get("agency"),
            "team_conf": player.get("team_conf"),
        })

    def build_team_side(comp) -> dict:
        team = comp.get("team") or {}
        team_id = team.get("id")
        try:
            team_id_int = int(team_id)
        except (TypeError, ValueError):
            team_id_int = None
        wins, losses = parse_record(comp.get("records"))
        meta = teams_lookup.get(team_id_int, {})
        conference = team.get("conferenceAbbreviation") or meta.get("shortConferenceName")
        entry = {
            "team_id": team_id_int,
            "name": team.get("displayName") or meta.get("displayName"),
            "location": team.get("location") or meta.get("location"),
            "abbrev": team.get("abbreviation") or meta.get("abbreviation"),
            "logo": team.get("logo") or meta.get("logo"),
            "conference": conference,
            "conference_full": meta.get("conferenceName"),
            "color": meta.get("color"),
            "alternate_color": meta.get("alternateColor"),
            "is_power": conference in power_confs if conference else False,
        }
        if wins is not None and losses is not None:
            entry["record"] = {"wins": wins, "losses": losses}
        rank_obj = comp.get("curatedRank") or {}
        entry["rank"] = rank_obj.get("current") or comp.get("rank")
        return entry

    outputs = []

    for schedule_date in schedule_dates:
        try:
            schedule_dt = datetime.strptime(schedule_date, "%Y-%m-%d")
        except ValueError:
            continue

        date_token = schedule_dt.strftime(DATE_FORMAT)
        scoreboard_url = SCOREBOARD_URL_TEMPLATE_WOMEN(date_token) if women else SCOREBOARD_URL_TEMPLATE(date_token)
        scoreboard = fetch_data(scoreboard_url, context)

        raw_path = os.path.join(storage.filepath, "schedule", "women" if women else "men", schedule_date)
        os.makedirs(raw_path, exist_ok=True)
        raw_file = os.path.join(raw_path, "scoreboard.json")
        with open(raw_file, "w") as f:
            json.dump(scoreboard, f)

        games: list[dict] = []
        started_games = 0

        events = scoreboard.get("events") or []
        for event in events:
            competition = (event.get("competitions") or [{}])[0]
            status_type = (competition.get("status") or {}).get("type") or {}
            state = status_type.get("state", "").lower()

            if state not in {"pre", "scheduled", ""}:
                started_games += 1

            competitors = competition.get("competitors") or []
            home = next((c for c in competitors if c.get("homeAway") == "home"), None)
            away = next((c for c in competitors if c.get("homeAway") == "away"), None)
            if not home or not away:
                continue

            home_team = build_team_side(home)
            away_team = build_team_side(away)

            odds_raw = competition.get("odds") or []
            primary_odds = odds_raw[0] if odds_raw else {}
            odds = {
                "provider": ((primary_odds.get("provider") or {}).get("name")),
                "details": primary_odds.get("details"),
                "spread": safe_float(primary_odds.get("spread")),
                "over_under": safe_float(primary_odds.get("overUnder")),
                "home_moneyline": safe_int((primary_odds.get("homeTeamOdds") or {}).get("moneyLine")),
                "away_moneyline": safe_int((primary_odds.get("awayTeamOdds") or {}).get("moneyLine")),
            }
            if (primary_odds.get("homeTeamOdds") or {}).get("favorite"):
                odds["favorite"] = "home"
            elif (primary_odds.get("awayTeamOdds") or {}).get("favorite"):
                odds["favorite"] = "away"
            if not any(val is not None for val in odds.values()):
                odds = None

            broadcast = None
            broadcasts = competition.get("broadcasts") or []
            if broadcasts:
                names = broadcasts[0].get("names") or []
                broadcast = names[0] if names else broadcasts[0].get("shortName")

            start_iso = competition.get("date") or event.get("date")
            start_dt = iso_dt(start_iso)

            venue_obj = competition.get("venue") or {}
            venue_addr = venue_obj.get("address") or {}
            venue_city = venue_addr.get("city")
            venue_state = venue_addr.get("state") or venue_addr.get("zipCode")
            venue_country = venue_addr.get("country")
            venue_location_parts = [p for p in [venue_city, venue_state] if p]
            venue_location = ", ".join(venue_location_parts) if venue_location_parts else (venue_country or None)

            home_list = team_players.get(home_team.get("team_id"), [])
            away_list = team_players.get(away_team.get("team_id"), [])
            home_featured = home_list if not max_per_team else home_list[:max_per_team]
            away_featured = away_list if not max_per_team else away_list[:max_per_team]
            featured = sorted(home_featured + away_featured, key=lambda p: p["overall_rank"])
            if max_featured:
                featured = featured[:max_featured]

            try:
                game_id = int(event.get("id"))
            except (TypeError, ValueError):
                game_id = event.get("id")

            games.append(
                {
                    "game_id": game_id,
                    "start_time": start_dt.isoformat() if start_dt else start_iso,
                    "status": status_type.get("detail") or status_type.get("description"),
                    "neutral_site": bool(competition.get("neutralSite")),
                    "venue": venue_obj.get("fullName"),
                    "venue_city": venue_city,
                    "venue_state": venue_state,
                    "venue_country": venue_country,
                    "venue_location": venue_location,
                    "broadcast": broadcast,
                    "odds": odds,
                    "home": home_team,
                    "away": away_team,
                    "featured_players": featured,
                    "conference": home_team.get("conference") or away_team.get("conference"),
                    "url": event.get("links", [{}])[0].get("href"),
                    "_sort": start_dt.timestamp() if start_dt else None,
                }
            )

        games.sort(key=lambda g: (g.get("_sort") is None, g.get("_sort"), g.get("game_id")))
        for game in games:
            game.pop("_sort", None)

        schedule_dir = os.path.join(web_root, "women" if women else "men", "schedule")
        os.makedirs(schedule_dir, exist_ok=True)
        output_file = os.path.join(schedule_dir, f"{schedule_date}.json")

        output = {
            "meta": {
                "schedule_date": schedule_date,
                "rankings_date": rankings_date,
                "generated_at": datetime.now().isoformat(),
                "gender": "women" if women else "men",
                "total_games": len(games),
                "skipped_started": started_games,
            },
            "games": games,
        }

        with open(output_file, "w") as f:
            json.dump(output, f, indent=2, default=str)
        maybe_upload_json(output_file, f"{'women' if women else 'men'}/schedule/{schedule_date}.json")
        context.log.info(f"Wrote schedule with {len(games)} games to {output_file}")

        outputs.append(
            {
                "schedule_date": schedule_date,
                "output_file": output_file,
                "num_games": len(games),
                "skipped_started": started_games,
            }
        )

    return MaterializeResult(
        metadata={
            "outputs": MetadataValue.json(outputs),
            "rankings_date": MetadataValue.text(rankings_date),
            "schedule_dates": MetadataValue.json(schedule_dates),
        }
    )


@asset(
    deps=["web_season_rankings_json"],
    config_schema={
        "rankings_date": Field(Noneable(String), is_required=False, default_value=None),
        "end_date": Field(Noneable(String), is_required=False, default_value=None),
        "base_rating": Field(Int, default_value=1500, is_required=False),
        "k_factor": Field(Int, default_value=24, is_required=False),
        "player_limit": Field(Int, default_value=500, is_required=False),
    },
    group_name=WEB_EXPORT,
    compute_kind=PYTHON
)
def web_votes_elo_json(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """
    Aggregate vote JSON blobs into Elo ratings for men's prospects.
    Uses a capped pool from the season rankings snapshot.
    Output: data/web/men/elo_rankings.json
    """
    config = context.op_config
    base_rating = config.get("base_rating")
    k_factor = config.get("k_factor")
    player_limit = config.get("player_limit")
    rankings_date = config.get("rankings_date") or config.get("end_date")

    web_data_path = os.path.join(
        os.environ.get('DAGSTER_HOME', '.'),
        "data", "web"
    )

    # Infer rankings date if not provided
    if not rankings_date:
        manifest_path = os.path.join(web_data_path, "manifest.json")
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, "r") as f:
                    manifest = json.load(f)
                rankings_date = manifest.get("men", {}).get("rankings", [manifest.get("latest_date")])[0]
            except Exception:
                rankings_date = None
    # Fallback: pick the latest rankings file on disk
    if not rankings_date:
        rankings_dir = Path(web_data_path) / "men" / "rankings"
        if rankings_dir.exists():
            ranking_files = sorted(rankings_dir.glob("*.json"))
            if ranking_files:
                rankings_date = ranking_files[-1].stem
    if not rankings_date:
        raise ValueError("rankings_date not provided and could not infer from manifest.")

    rankings_data = None
    env, bucket, prefix, votes_prefix = s3_config()
    rankings_key = f"{prefix}men/rankings/{rankings_date}.json"

    if env == "PROD" and bucket:
        try:
            obj = boto3.client("s3").get_object(Bucket=bucket, Key=rankings_key)
            rankings_data = json.loads(obj["Body"].read())
        except Exception as e:
            context.log.warn(f"Failed to load rankings from S3 ({rankings_key}): {e}")

    if rankings_data is None:
        rankings_file = os.path.join(web_data_path, "men", "rankings", f"{rankings_date}.json")
        if not os.path.exists(rankings_file):
            raise FileNotFoundError(f"Rankings file not found: {rankings_file}")
        with open(rankings_file, "r") as f:
            rankings_data = json.load(f)

    players = rankings_data.get("players", [])

    pool = list(players)
    pool.sort(
        key=lambda p: (
            p.get("ez") is None,
            -(
                (p.get("ez") or 0)
                / (p.get("gp") or 1)
            ),
        )
    )
    if player_limit:
        pool = pool[:player_limit]

    player_lookup = {p["player_id"]: p for p in pool}
    ratings = {pid: float(base_rating) for pid in player_lookup}
    stats = {pid: {"wins": 0, "losses": 0, "votes": 0, "last_vote_at": None} for pid in player_lookup}

    vote_records = []
    skipped_invalid = 0
    skipped_pool = 0
    skipped_gender = 0
    distinct_voters = set()
    total_vote_files = 0
    latest_vote_ts = None

    # Load votes
    if env == "PROD" and bucket:
        client = boto3.client("s3")
        continuation = None
        while True:
            params = {"Bucket": bucket, "Prefix": votes_prefix}
            if continuation:
                params["ContinuationToken"] = continuation
            resp = client.list_objects_v2(**params)
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                total_vote_files += 1
                try:
                    data = client.get_object(Bucket=bucket, Key=key)
                    record = json.loads(data["Body"].read())
                    ts = record.get("received_at") or record.get("timestamp")
                    vote_time = datetime.fromisoformat(ts) if ts else None
                    vote_records.append((vote_time, record))
                    if vote_time and (latest_vote_ts is None or vote_time > latest_vote_ts):
                        latest_vote_ts = vote_time
                except Exception:
                    skipped_invalid += 1
            if resp.get("IsTruncated"):
                continuation = resp.get("NextContinuationToken")
            else:
                break
    else:
        votes_path = os.path.join(web_data_path, "votes")
        if os.path.exists(votes_path):
            for root, _, files in os.walk(votes_path):
                for fname in files:
                    if not fname.endswith(".json"):
                        continue
                    fpath = os.path.join(root, fname)
                    total_vote_files += 1
                    try:
                        with open(fpath, "r") as f:
                            record = json.load(f)
                        ts = record.get("received_at") or record.get("timestamp")
                        vote_time = datetime.fromisoformat(ts) if ts else None
                        vote_records.append((vote_time, record))
                        if vote_time and (latest_vote_ts is None or vote_time > latest_vote_ts):
                            latest_vote_ts = vote_time
                    except Exception:
                        skipped_invalid += 1

    # Sort votes chronologically to keep Elo updates deterministic
    vote_records.sort(key=lambda x: (x[0] or datetime.min))

    processed_votes = 0
    for vote_time, record in vote_records:
        if record.get("gender", "men") != "men":
            skipped_gender += 1
            continue
        pa = record.get("player_a_id")
        pb = record.get("player_b_id")
        winner = record.get("winner_id")
        if pa not in ratings or pb not in ratings:
            skipped_pool += 1
            continue
        if winner not in (pa, pb):
            skipped_invalid += 1
            continue

        ra = ratings[pa]
        rb = ratings[pb]
        expected_a = 1.0 / (1.0 + 10 ** ((rb - ra) / 400))
        score_a = 1.0 if winner == pa else 0.0
        score_b = 1.0 - score_a

        ratings[pa] = ra + k_factor * (score_a - expected_a)
        ratings[pb] = rb + k_factor * (score_b - (1 - expected_a))

        stats[pa]["wins" if winner == pa else "losses"] += 1
        stats[pb]["wins" if winner == pb else "losses"] += 1
        stats[pa]["votes"] += 1
        stats[pb]["votes"] += 1
        iso_ts = (vote_time or datetime.utcnow()).isoformat()
        stats[pa]["last_vote_at"] = iso_ts
        stats[pb]["last_vote_at"] = iso_ts

        ip_hash = record.get("ip_hash")
        if ip_hash:
            distinct_voters.add(ip_hash)

        processed_votes += 1

    output_players = []
    for pid, player in player_lookup.items():
        record = {
            "player_id": pid,
            "name": player.get("display_name") or player.get("full_name"),
            "team": player.get("team_location") or player.get("team_name"),
            "recruit_rank": player.get("recruit_rank"),
            "class": player.get("experience_display_value"),
            "position": player.get("position_display_name"),
            "headshot": player.get("headshot_href"),
            "rating": round(ratings[pid], 2),
            "wins": stats[pid]["wins"],
            "losses": stats[pid]["losses"],
            "votes": stats[pid]["votes"],
            "last_vote_at": stats[pid]["last_vote_at"],
            "ez": player.get("ez"),
        }
        output_players.append(record)

    output_players.sort(key=lambda p: p["rating"], reverse=True)

    output = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "rankings_date": rankings_date,
            "k_factor": k_factor,
            "base_rating": base_rating,
            "player_limit": player_limit,
            "player_pool": len(output_players),
            "total_votes": processed_votes,
            "distinct_voters": len(distinct_voters),
            "skipped_invalid": skipped_invalid,
            "skipped_out_of_pool": skipped_pool,
            "skipped_wrong_gender": skipped_gender,
        },
        "players": output_players
    }

    output_file = os.path.join(web_data_path, "men", "elo_rankings.json")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    maybe_upload_json(output_file, "men/elo_rankings.json")

    context.log.info(f"Wrote Elo rankings to {output_file}")

    return MaterializeResult(
        metadata={
            "output_file": MetadataValue.path(output_file),
            "rankings_date": MetadataValue.text(rankings_date),
            "player_pool": MetadataValue.int(len(output_players)),
            "total_votes": MetadataValue.int(processed_votes),
            "distinct_voters": MetadataValue.int(len(distinct_voters)),
            "skipped_invalid": MetadataValue.int(skipped_invalid),
            "skipped_out_of_pool": MetadataValue.int(skipped_pool),
            "skipped_wrong_gender": MetadataValue.int(skipped_gender),
            "vote_files": MetadataValue.int(total_vote_files),
            "latest_vote_at": MetadataValue.text(latest_vote_ts.isoformat() if latest_vote_ts else "unknown"),
        }
    )


@asset(
    deps=["web_daily_report_json", "web_season_rankings_json", "web_conferences_json", "web_votes_elo_json", "web_schedule_json"],
    config_schema={
        "end_date": Field(Noneable(String), default_value=None, is_required=False),
    },
    group_name=WEB_EXPORT,
    compute_kind=PYTHON
)
def web_manifest_json(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """
    Generate a manifest file that lists all available data files.
    The Vue.js app can fetch this to know what data is available.
    Output: data/web/manifest.json
    """
    end_date = context.op_config.get('end_date') or default_end_date()
    
    web_data_path = os.path.join(
        os.environ.get('DAGSTER_HOME', '.'),
        "data", "web"
    )

    # Scan for available data files
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "latest_date": end_date,
        "men": {
            "daily": [],
            "rankings": [],
            "schedule": [],
            "toplines": [],
            "prospects": None,
            "conferences": None,
            "elo_rankings": None,
        },
        "women": {
            "daily": [],
            "rankings": [],
            "schedule": [],
            "toplines": [],
            "prospects": None,
            "conferences": None,
            "elo_rankings": None,
        }
    }

    for gender in ["men", "women"]:
        gender_path = os.path.join(web_data_path, gender)
        
        # Check for daily reports - now organized by date with _1d, _2d, _3d, _7d suffixes
        daily_path = os.path.join(gender_path, "daily")
        if os.path.exists(daily_path):
            # Find unique dates from files like 2025-12-20_1d.json
            daily_files = [f.replace(".json", "") for f in os.listdir(daily_path) if f.endswith(".json")]
            # Extract unique dates (remove the _Xd suffix)
            unique_dates = sorted(set([
                f.rsplit("_", 1)[0] for f in daily_files if "_" in f
            ]), reverse=True)
            manifest[gender]["daily"] = unique_dates
        
        # Check for rankings
        rankings_path = os.path.join(gender_path, "rankings")
        if os.path.exists(rankings_path):
            manifest[gender]["rankings"] = sorted([
                f.replace(".json", "") 
                for f in os.listdir(rankings_path) 
                if f.endswith(".json")
            ], reverse=True)

        schedule_path = os.path.join(gender_path, "schedule")
        if os.path.exists(schedule_path):
            manifest[gender]["schedule"] = sorted([
                f.replace(".json", "")
                for f in os.listdir(schedule_path)
                if f.endswith(".json")
            ], reverse=True)

        toplines_path = os.path.join(gender_path, "toplines")
        if os.path.exists(toplines_path):
            manifest[gender]["toplines"] = sorted([
                f.replace(".json", "")
                for f in os.listdir(toplines_path)
                if f.endswith(".json")
            ], reverse=True)
        
        # Check for prospects and conferences
        if os.path.exists(os.path.join(gender_path, "prospects.json")):
            manifest[gender]["prospects"] = "prospects.json"
        if os.path.exists(os.path.join(gender_path, "conferences.json")):
            manifest[gender]["conferences"] = "conferences.json"
        if os.path.exists(os.path.join(gender_path, "elo_rankings.json")):
            manifest[gender]["elo_rankings"] = "elo_rankings.json"

    output_file = os.path.join(web_data_path, "manifest.json")
    with open(output_file, "w") as f:
        json.dump(manifest, f, indent=2)
    maybe_upload_json(output_file, "manifest.json")
    
    context.log.info(f"Wrote manifest to {output_file}")

    return MaterializeResult(
        metadata={
            "output_file": MetadataValue.path(output_file),
            "manifest": MetadataValue.json(manifest)
        }
    )
