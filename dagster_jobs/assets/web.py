"""
Dagster assets for generating JSON data files for the Top Lines web app.
These assets query the same data as the HTML reports but output JSON
that can be consumed by the Vue.js frontend.
"""

import json
import os
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
from .constants import PYTHON, DUCKDB, WEB_EXPORT, SEASON_START_DATE

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
    deps=["web_daily_report_json", "web_season_rankings_json", "web_conferences_json", "web_votes_elo_json"],
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
            "prospects": None,
            "conferences": None,
            "elo_rankings": None,
        },
        "women": {
            "daily": [],
            "rankings": [],
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
