def teams_metadata(path: str) -> str:
    """
    read-only query for teams file
    """
    return f"""
    select
        season,
        name as conference,
        midsizeName as shortConference,
        unnest(teams, recursive:=true)
    from
        (select
            season.year as season,
            unnest(groups, recursive:=true)
        from
            (select
                unnest(leagues[1])
            from
            (select unnest(sports[1], recursive:=true) from read_json('{path}'))));
    """

def conferences_metadata(path: str) -> str:
    """
    read-only query for teams file
    """
    return f"""
    select
        groupId as id,
        name,
        shortName,
        logo
    from	
    (
        select unnest(conferences, recursive:=true) as group from read_json('{path}')
    );
    """

def create_table_stage_conferences() -> str:
    """
    create the conferences table
    """
    return """
    create table if not exists stage_conferences (
        id INTEGER PRIMARY KEY,
        name STRING,
        shortName STRING,
        logo STRING
    );
    """

def create_table_stage_players() -> str:
    """
    create players staging table
    """
    return """
    create table if not exists stage_players (
        id INT PRIMARY KEY,
        team_id INT,
        guid UUID,
        first_name STRING,
        last_name STRING,
        full_name STRING,
        display_name STRING,
        short_name STRING,
        weight DOUBLE,
        display_weight STRING,
        height DOUBLE,
        display_height STRING,
        city STRING,
        state STRING,
        country STRING,
        slug STRING,
        headshot_href STRING,
        jersey INT,
        position_id TINYINT,
        position_name STRING,
        position_display_name STRING,
        position_abbreviation STRING,
        experience_years TINYINT,
        experience_display_value STRING,
        experience_abbreviation STRING,
        status_id TINYINT,
        status_name STRING
    );
    """

def create_table_stage_teams() -> str:
    """
    create the teams table
    """
    return """
    create table if not exists stage_teams (
        season INT,
        conferenceName STRING,
        shortConferenceName STRING,
        id INTEGER PRIMARY KEY,
        uid STRING,
        slug STRING,
        abbreviation STRING,
        displayName STRING,
        shortDisplayName STRING,
        name STRING,
        nickname STRING,
        location STRING,
        color STRING,
        alternateColor STRING,
        isActive BOOLEAN,
        isAllStar BOOLEAN,
        logos STRUCT(href VARCHAR, alt VARCHAR, rel VARCHAR[], width BIGINT, height BIGINT)[],
        links STRUCT("language" VARCHAR, rel VARCHAR[], href VARCHAR, "text" VARCHAR, shortText VARCHAR, isExternal BOOLEAN, isPremium BOOLEAN, isHidden BOOLEAN)[]
    )
    """

def insert_table_stage_conferences(path: str) -> str:
    """
    insert conference data into staging table
    """
    return f"""
    insert or ignore into stage_conferences
    select
        groupId as id,
        name,
        shortName,
        logo
    from	
    (
        select unnest(conferences, recursive:=true) as group from read_json('{path}')
    )
    returning id, shortName;
    """

def insert_table_stage_players(path: list[str]) -> str:
    """
    insert players into staging table
    """
    return f"""
    insert or ignore into stage_players
    select
        id::INT as id,
        team_id::INT as team_id,
        guid,
        firstName as first_name,
        lastName as last_name,
        fullName as full_name,
        displayName as display_name,
        shortName as short_name,
        weight,
        displayWeight as display_weight,
        height,
        displayHeight as display_height,
        birthplace.city as city,
        birthplace.state as state,
        birthplace.country as country,
        slug,
        headshot.href as headshot_href,
        jersey::INT as jersey,
        position.id::TINYINT as position_id,
        position.name as position_name,
        position.displayName as position_display_name,
        position.abbreviation as position_abbreviation,
        experience.years::TINYINT as experience_years,
        experience.displayValue as experience_display_value,
        experience.abbreviation as experience_abbreviation,
        status.id::TINYINT as status_id,
        status.name as status_name
    from
    (select
        id as team_id,
        unnest(athletes, max_depth:=2) as athlete
    from (
    select unnest(team, recursive:=true) as team from read_json_auto({path}, union_by_name=true)
    ))
    returning id, team_id, display_name, display_height, display_weight, position_display_name;    
    """

def insert_table_stage_teams(path: str) -> str:
    """
    insert teams data into staging table
    """
    return f"""
    insert or ignore into stage_teams
    select
        season,
        name as conferenceName,
        midsizeName as shortConferenceName,
        unnest(teams, recursive:=true)
    from
    (select
        season.year as season,
        unnest(groups, recursive:=true)
    from
    (
    select
        unnest(leagues[1])
    from
    (
        select unnest(sports[1], recursive:=true) from read_json('{path}')))
    )
    returning season, id, displayName, conferenceName;
    """

def scoreboard_metadata(path: str) -> str:
    """
    query some fields from the daily scoreboard files and use them for markdown preview
    """
    return f"""
    select
        event.id,
        event.date,
        season.year,
        event.name,
        event.season.slug,
        event.status.period,
        event.status.type.completed,
        event.links[2].href as url
    from (
    select
        leagues[1].season as season,
         unnest(events) as event
    from read_json('{path}')
    );
    """

def create_table_stage_daily_scoreboard() -> str: 
    """
    create staging table for daily scoreboard
    """
    return """
    create table if not exists stage_daily_scoreboard (
        game_id INTEGER PRIMARY KEY,
        date DATE,
        season INTEGER,
        season_type STRING,
        matchup STRING,
        period TINYINT,
        minutes TINYINT,
        completed BOOLEAN,
        team_1_id INTEGER,
        team_1_info STRUCT(logo VARCHAR, "location" VARCHAR, "name" VARCHAR, abbrev VARCHAR, conf_id INTEGER, score INTEGER, homeAway VARCHAR, winner BOOLEAN, wins INTEGER, losses INTEGER),
        team_2_id INTEGER,
        team_2_info STRUCT(logo VARCHAR, "location" VARCHAR, "name" VARCHAR, abbrev VARCHAR, conf_id INTEGER, score INTEGER, homeAway VARCHAR, winner BOOLEAN, wins INTEGER, losses INTEGER),
    );
    """

def insert_table_stage_daily_scoreboard(path:str, date: str) -> str:
    """
    transform scoreboard data from files and load into staging table
    """
    return f"""
    insert or ignore into stage_daily_scoreboard 
        select 
            game_id,
            '{date}'::DATE, 
            season,
            season_type,
            matchup,
            period,
            40+5*(period-2) as minutes,
            completed,
            competitions.competitors[1].team.id::INT as team_1_id,
            struct_pack(
                logo := competitions.competitors[1].team.logo,
                location := competitions.competitors[1].team.location,
                name := competitions.competitors[1].team.name,
                abbrev := competitions.competitors[1].team.abbreviation,
                conf_id  := competitions.competitors[1].team.conferenceId::INT,
                score := competitions.competitors[1].score::INT,
                homeAway := competitions.competitors[1].homeAway,
                winner := competitions.competitors[1].winner::BOOL,
                wins := regexp_extract(competitions.competitors[1].records[1].summary,'([0-9]+)-',1)::INT,	
                losses := regexp_extract(competitions.competitors[1].records[1].summary,'-([0-9]+)',1)::INT	
            ) as team_1_info,
            competitions.competitors[2].team.id::INT as team_2_id,
            struct_pack(
                logo := competitions.competitors[2].team.logo,
                location := competitions.competitors[2].team.location,
                name := competitions.competitors[2].team.name,
                abbrev := competitions.competitors[2].team.abbreviation,
                conf_id  := competitions.competitors[2].team.conferenceId::INT,
                score := competitions.competitors[2].score::INT,
                homeAway := competitions.competitors[2].homeAway,
                winner := competitions.competitors[2].winner::BOOL,
                wins := regexp_extract(competitions.competitors[2].records[1].summary,'([0-9]+)-',1)::INT,	
                losses := regexp_extract(competitions.competitors[2].records[1].summary,'-([0-9]+)',1)::INT	
            ) as team_2_info
        from
        (select 
            games.id::INT game_id, 
            games.season.year::INT as season,
            games.season.slug as season_type,
            games.name as matchup, 
            games.status.period::TINYINT as period,
            games.status.type.completed as completed, 
            unnest(games.competitions) as competitions
        from (
            select 
                unnest(events) as games 
            from read_json('{path}')
            )
        )
        where completed is true
        returning game_id;
    """

def fetch_completed_game_ids(date:str) -> str:
    """
    return game_ids from staging table
    """
    return f"""
        select 
            game_id
        from stage_daily_scoreboard
        where (date = '{date}') 
            and (completed is true);
    """

def create_table_stage_game_logs() -> str: 
    """
    create staging table for game logs
    """
    return """
    create table if not exists stage_game_logs (
        game_id INTEGER PRIMARY KEY,
        date DATE,
        season INTEGER,
        minutes TINYINT,
        poss TINYINT,
        team_1_location STRING,
        team_1_id INTEGER,
        team_1_logo STRING,
        team_1_name STRING,
        team_1_display_name STRING,
        team_2_location STRING,
        team_2_id INTEGER,
        team_2_logo STRING,
        team_2_name STRING,
        team_2_display_name STRING,
        team_1_stats STRUCT(
            fgm INT, 
            fga INT, 
            fg3m INT, 
            fg3a INT, 
            ftm INT, 
            fta INT, 
            orb INT, 
            drb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT),
        team_2_stats STRUCT(
            fgm INT, 
            fga INT, 
            fg3m INT, 
            fg3a INT, 
            ftm INT, 
            fta INT, 
            orb INT, 
            drb INT, 
            ast INT, 
            stl INT, 
            blk INT, 
            tov INT, 
            pf INT),
        team_1_pts INTEGER,
        team_2_pts INTEGER
    );
    """

def insert_table_stage_game_logs(files:list[str]) -> str:
    """
    insert box score totals into staging table
    """
    return f"""
    insert or ignore into stage_game_logs
        with s as (
        select
            header.id::INTEGER as game_id,
            boxscore.teams[1].team.id as team_1_id,
            boxscore.teams[1].team.location as team_1_location,
            boxscore.teams[1].team.logo as team_1_logo,
            boxscore.teams[1].team.name as team_1_name,
            boxscore.teams[1].team.displayName as team_1_display_name,
            boxscore.teams[2].team.id as team_2_id,
            boxscore.teams[2].team.location as team_2_location,
            boxscore.teams[2].team.logo as team_2_logo,
            boxscore.teams[2].team.name as team_2_name,
            boxscore.teams[2].team.displayName as team_2_display_name,
            struct_pack(
                fgm := regexp_extract(boxscore.teams[1].statistics[1].displayValue, '([0-9]+)-',1)::INT,
                fga := regexp_extract(boxscore.teams[1].statistics[1].displayValue, '-([0-9]+)',1)::INT,
                fg3m := regexp_extract(boxscore.teams[1].statistics[3].displayValue, '([0-9]+)-',1)::INT,
                fg3a := regexp_extract(boxscore.teams[1].statistics[3].displayValue, '-([0-9]+)',1)::INT,
                ftm := regexp_extract(boxscore.teams[1].statistics[5].displayValue, '([0-9]+)-',1)::INT,
                fta := regexp_extract(boxscore.teams[1].statistics[5].displayValue, '-([0-9]+)',1)::INT,
                orb := boxscore.teams[1].statistics[8].displayValue::INT,
                drb := boxscore.teams[1].statistics[9].displayValue::INT,
                ast := boxscore.teams[1].statistics[10].displayValue::INT,
                stl := boxscore.teams[1].statistics[11].displayValue::INT,
                blk := boxscore.teams[1].statistics[12].displayValue::INT,
                tov := boxscore.teams[1].statistics[15].displayValue::INT,
                pf := boxscore.teams[1].statistics[22].displayValue::INT
                ) as team_1_stats,
            struct_pack(
                fgm := regexp_extract(boxscore.teams[2].statistics[1].displayValue, '([0-9]+)-',1)::INT,
                fga := regexp_extract(boxscore.teams[2].statistics[1].displayValue, '-([0-9]+)',1)::INT,
                fg3m := regexp_extract(boxscore.teams[2].statistics[3].displayValue, '([0-9]+)-',1)::INT,
                fg3a := regexp_extract(boxscore.teams[2].statistics[3].displayValue, '-([0-9]+)',1)::INT,
                ftm := regexp_extract(boxscore.teams[2].statistics[5].displayValue, '([0-9]+)-',1)::INT,
                fta := regexp_extract(boxscore.teams[2].statistics[5].displayValue, '-([0-9]+)',1)::INT,
                orb := boxscore.teams[2].statistics[8].displayValue::INT,
                drb := boxscore.teams[2].statistics[9].displayValue::INT,
                ast := boxscore.teams[2].statistics[10].displayValue::INT,
                stl := boxscore.teams[2].statistics[11].displayValue::INT,
                blk := boxscore.teams[2].statistics[12].displayValue::INT,
                tov := boxscore.teams[2].statistics[15].displayValue::INT,
                pf := boxscore.teams[2].statistics[22].displayValue::INT
                ) as team_2_stats
        from read_json({files})
        )
        select 
            s.game_id,
            sds.date,
            sds.season,
            sds.minutes,
            round(0.5*(team_1_stats.fga+team_2_stats.fga)+0.44*(team_1_stats.fta+team_2_stats.fta)-
                (team_1_stats.orb+team_2_stats.orb)+(team_1_stats.tov+team_2_stats.tov)) as poss,
            s.team_1_location,
            s.team_1_id,
            s.team_1_logo,
            s.team_1_name,
            s.team_1_display_name,
            s.team_2_location,
            s.team_2_id,
            s.team_2_logo,
            s.team_2_name,
            s.team_2_display_name, 
            team_1_stats,
            team_2_stats,
            team_1_stats.ftm + 3*team_1_stats.fg3m + 2*(team_1_stats.fgm-team_1_stats.fg3m) as team_1_pts,
            team_2_stats.ftm + 3*team_2_stats.fg3m + 2*(team_2_stats.fgm-team_2_stats.fg3m) as team_2_pts
        from s join stage_daily_scoreboard sds on s.game_id=sds.game_id
        returning game_id;
    """

def create_table_stage_player_lines() -> str:
    """
    staging table for player lines (eg box score totals)
    """

    return f"""
        create table if not exists stage_player_lines (
            game_id INT,
            date DATE,
            player_id INT,
            season INT,
            opp_id INT,
            team_id INT,
            home BOOL,
            name STRING,
            player_url STRING,
            img_url STRING,
            starter BOOL,
            jersey TINYINT,
            stats STRUCT("minutes" INTEGER, fgm INTEGER, fga INTEGER, fg3m INTEGER, fg3a INTEGER, ftm INTEGER, fta INTEGER, orb INTEGER, drb INTEGER, reb INTEGER, ast INTEGER, stl INTEGER, blk INTEGER, tov INTEGER, pf INTEGER, pts INTEGER),
            PRIMARY KEY (game_id, player_id)
        )
    """

def insert_table_stage_player_lines(files:list[str], date:str) -> str:
    """
    insert data into staging table for player lines
    """
    
    return f"""
    insert or ignore into stage_player_lines
        select 
            game_id,
            '{date}'::DATE,
            id::INT as player_id,
            season,
            case 
                when displayOrder=1 then team_2_id
                else team_1_id
            end as opp_id,
            case
                when displayOrder=1 then team_1_id
                else team_2_id
            end as team_id,
            case 
                when displayOrder=1 then false
                else true
            end as home,
            displayName as name,
            links[1].href as player_url,
            href as img_url,
            starter,
            jersey::TINYINT as jersey,
            struct_pack(
                minutes := stats[1]::INT,
                fgm := regexp_extract(stats[2], '([0-9]+)-',1)::INT,
                fga := regexp_extract(stats[2], '-([0-9]+)',1)::INT,
                fg3m := regexp_extract(stats[3], '([0-9]+)-',1)::INT,
                fg3a := regexp_extract(stats[3], '-([0-9]+)',1)::INT,
                ftm := regexp_extract(stats[4], '([0-9]+)-',1)::INT,
                fta := regexp_extract(stats[4], '-([0-9]+)',1)::INT,
                orb := stats[5]::INT,
                drb := stats[6]::INT,
                reb := stats[7]::INT, 
                ast := stats[8]::INT,
                stl := stats[9]::INT,
                blk := stats[10]::INT,
                tov := stats[11]::INT,
                pf := stats[12]::INT,
                pts := stats[13]::INT
            ) as stats
        from
        (select 
            game_id,
            season,
            team_1_id,
            team_2_id,
            players.displayOrder,
            unnest(players.statistics[1].athletes, recursive:=true)
        from
        (
        select 
                header.id::INT as game_id, 
                header.season.year as season,
                boxscore.teams[1].team.id::INT as team_1_id,
                boxscore.teams[2].team.id::INT as team_2_id,
                unnest(boxscore['players']) as players 
        from read_json({files})
        ))
        where stats[1] is not null
        returning player_id;
    """

def create_table_stage_plays() -> str:
    """"
    create table for staging play-by-play
    """
    return """
    create table if not exists stage_plays (
        play_id BIGINT,
        sequence_id INT,
        game_id INT,
        team_id INT,
        opp_id INT,
        home BOOL,
        date DATE,
        season INT,
        play_index INT,
        type_id INT,
        type_text STRING,
        text STRING,
        shot STRUCT(player STRING, result STRING, "event" STRING),
        assist STRING,
        event STRUCT(player STRING, "event" STRING),
        foul STRUCT("event" STRING, player STRING),
        awayScore INT,
        homeScore INT,
        period TINYINT,
        game_clock STRING,
        scoringPlay BOOL,
        wallclock TIMESTAMP,
        shootingPlay BOOL,
        player_1_id INT,
        player_2_id INT,
        PRIMARY KEY (play_id)
    );
    """

def insert_table_stage_plays(files:list[str], date:str) -> str:
    """
    insert plays into staging table
    """
    return f"""
    insert or ignore into stage_plays
        select 
            id::BIGINT as play_id,
            sequenceNumber::INT as sequence_id,
            game_id::INT as game_id,
            team.id::INT as team_id,
            case
                when team.id=team_1_id then team_2_id else team_1_id end 
            as opp_id,
            case
                when team.id=team_1_id then false else true end
            as home,
            '{date}'::DATE as date,
            season,
            index::INT as play_index,
            type.id::INT as type_id,
            type.text as type_text,
            text,
            regexp_extract(text,'(?P<player>[\W\w\s]+) (?P<result>missed|made) (?P<event>[\w\s]+).', ['player','result','event']) as shot,
            nullif(regexp_extract(text, 'Assisted by ([\w\s\W]+).$', 1), '') as assist,
            regexp_extract(text, '(?P<player>[\w\s\W]+) (?P<event>Offensive Rebound|Defensive Rebound|Turnover|Steal|Block).$', ['player', 'event']) as event,
            regexp_extract(text, '(?P<event>Foul) on (?P<player>[\w\s\W]+).$', ['event', 'player']) as foul,
            awayScore,
            homeScore,
            period.number::TINYINT as period,
            clock.displayValue as game_clock,
            scoringPlay,
            wallClock,
            shootingPlay,
            participants[1].athlete.id::INT as player_1_id,
            participants[2].athlete.id::INT as player_2_id
        from
        (
        select 
            unnest(plays, max_depth := 2) as play,
            header.id as game_id,
            header.season.year::INT as season,
            boxscore.teams[1].team.id as team_1_id,
            boxscore.teams[2].team.id as team_2_id,
            generate_subscripts(plays, 1) as index
        from read_json({files})
        )
        returning play_id;
    """

def insert_table_stage_kenpom(path: str) -> str:
    """
    query to insert values from kenpom file
    """
    return f"""
    insert or ignore into stage_kenpom
    select
        rank,
        team,
        ortg,
        drtg,
        year
    from read_csv('{path}')
    returning rank, team;
    """

def create_table_stage_player_shots_by_game() -> str:
    """
    for tracking shot types (dunks, layups, etc)
    """
    return """
    create table if not exists stage_player_shots_by_game (
        game_id INT,
        date DATE,
        team_id INT,
        opp_id INT,
        home BOOLEAN,
        player_id INT,
        ast_tip INT,
        unast_tip INT,
        miss_tip INT,
        ast_dunk INT,
        unast_dunk INT,
        miss_dunk INT,
        ast_layup INT,
        unast_layup INT,
        miss_layup INT,
        ast_mid INT,
        unast_mid INT,
        miss_mid INT,
        ast_3pt INT,
        unast_3pt INT,
        miss_3pt INT,
        PRIMARY KEY (game_id, player_id)
    );
    """

def insert_table_stage_player_shots_by_game(date: str) -> str:
    """
    count dunks, layups, mid-range and 3pt shots by player by game
    """
    return f"""
    insert or ignore into stage_player_shots_by_game
    select 
        game_id,
        date,
        team_id,
        opp_id,
        home,
        player_1_id as player_id,
        count(case when type_id=437 and shot.result='made' and assist is not null then 1 end) as ast_tip,
        count(case when type_id=437 and shot.result='made' and assist is null then 1 end) as unast_tip,
        count(case when type_id=437 and shot.result='missed' then 1 end) as miss_tip,    
        count(case when type_id=574 and shot.result='made' and assist is not null then 1 end) as ast_dunk,
        count(case when type_id=574 and shot.result='made' and assist is null then 1 end) as unast_dunk,
        count(case when type_id=574 and shot.result='missed' then 1 end) as miss_dunk,
        count(case when type_id=572 and shot.result='made' and assist is not null then 1 end) as ast_layup,
        count(case when type_id=572 and shot.result='made' and assist is null then 1 end) as unast_layup,
        count(case when type_id=572 and shot.result='missed' then 1 end) as miss_layup,
        count(case when type_id=558 and shot.event='Jumper' and shot.result='made' and assist is not null then 1 end) as ast_mid,
        count(case when type_id=558 and shot.event='Jumper' and shot.result='made' and assist is null then 1 end) as unast_mid,
        count(case when type_id=558 and shot.event='Jumper' and shot.result='missed' then 1 end) as miss_mid,
        count(case when type_id=558 and shot.event='Three Point Jumper' and shot.result='made' and assist is not null then 1 end) as ast_3pt,
        count(case when type_id=558 and shot.event='Three Point Jumper' and shot.result='made' and assist is null then 1 end) as unast_3pt,
        count(case when type_id=558 and shot.event='Three Point Jumper' and shot.result='missed' then 1 end) as miss_3pt
    from stage_plays
    where type_id in (437, 558, 572, 574) and
    date='{date}' and player_1_id is not null
    group by ALL
    returning game_id, team_id, opp_id, home, player_id;
    """

def create_table_stage_player_assists_by_game() -> str:
    """
    create table for staging player assist types
    """
    return """
    create table if not exists stage_player_assists_by_game (
        game_id INT,
        team_id INT,
        opp_id INT,
        home BOOLEAN,
        player_id INT,
        ast_to_dunk INT,
        ast_to_layup INT,
        ast_to_mid INT,
        ast_to_3pt INT,
        PRIMARY KEY (game_id, player_id)
    );
    """

def stage_player_assists_by_game(date: str) -> str:
    """
    count different types of assists
    """
    return f"""
    create or replace table stage_player_assists_by_game as
    select 
        game_id,
        team_id,
        opp_id,
        home,
        player_2_id as player_id,
        count(case when type_id=574 and shot.result='made' and assist is not null then 1 end) as ast_to_dunk,
        count(case when type_id=572 and shot.result='made' and assist is not null then 1 end) as ast_to_layup,
        count(case when type_id=558 and shot.event='Jumper' and shot.result='made' and assist is not null then 1 end) as ast_to_mid,
        count(case when type_id=558 and shot.event='Three Point Jumper' and shot.result='made' and assist is not null then 1 end) as ast_to_3pt
    from stage_plays
    where type_id in (558, 572, 574) and
    date='{date}'
    and player_2_id is not null
    group by ALL;
    """

def insert_table_stage_top_lines(start_date: str, end_date: str) -> str:
    return f"""
    create or replace table stage_top_lines as
    with lines as (
        select
            player_id,
            game_id,
            date,
            opp_id,
            team_id,
            home,
            name,
            player_url,
            img_url,
            starter,
            jersey,
            stats.minutes,
            stats.fgm,
            stats.fga,
            stats.fg3m,
            stats.fg3a,
            stats.ftm,
            stats.fta,
            stats.orb,
            stats.drb,
            stats.reb,
            stats.ast,
            stats.stl,
            stats.blk,
            stats.tov,
            stats.pf,
            stats.pts
        from stage_player_lines
    )
    select
        l.player_id,
        display_name,
        name,
        slug,
        player_url,
        img_url,
        p.jersey,
        display_weight,
        display_height,
        experience_abbreviation,
        starter,
        l.game_id,
        l.date,
        l.team_id,
        l.home,
        opp_id,
        stl,
        blk,
        fgm
        fga,
        fg3m,
        fg3a,
        tov,
        ast,
        fta,
        ftm,
        drb,
        orb,
        l.minutes,
        ast_dunk,
        unast_dunk,
        miss_dunk,
        ast_layup,
        unast_layup,
        miss_layup,
        ast_2pt,
        unast_2pt,
        ast_3pt,
        unast_3pt,
        g.minutes as game_minutes,
        poss,
        team_1_location,
        team_2_location,
        team_1_logo,
        team_2_logo,
        team_1_pts,
        team_2_pts,
        team_1_stats.fga as team_1_fga,
        team_1_stats.fta as team_1_fta,
        team_1_stats.tov as team_1_tov,
        team_2_stats.fga as team_2_fga,
        team_2_stats.fta as team_2_fta,
        team_2_stats.tov as team_2_tov,
        round(
            0.6*orb + 0.3*drb + 1.0*stl + 0.7*blk + 0.3*(ast_dunk+ast_2pt+ast_layup) +
            1.0*(unast_dunk+unast_layup+unast_2pt) +
            2.0*unast_3pt + 1.3*ast_3pt + (-0.7)*(fga-fgm) + 0.7*ast + 0.5*ftm - 0.5*(fta-ftm) + 
            0.1*fta + 0.1*(fga-fg3a) + 0.2*fg3a - 0.8*tov
        , 3) as ez
    from lines l join stage_player_shots_by_game s on l.game_id=s.game_id
    and l.player_id=s.player_id 
    join stage_game_logs g on l.game_id=g.game_id
    left join stage_players p on l.player_id=p.id
    where (g.date between '{start_date}' and '{end_date}')
    and team_1_logo is not null and team_2_logo is not null
    order by ez desc
    """