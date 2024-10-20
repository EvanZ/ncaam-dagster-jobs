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
    insert or ignore into stage_player_assists_by_game
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
    group by ALL
    returning game_id, team_id, player_id, ast_to_dunk, ast_to_layup, ast_to_mid, ast_to_3pt;
    """

def create_table_stage_top_lines() -> str:
    """
    table for storing daily player stats that will be used to create Top Lines reports
    """
    return f"""
        create table if not exists stage_top_lines (
            player_id INT,
            name VARCHAR,
            years TINYINT,
            game_id INT,
            date DATE,
            team_id INT,
            opp_id INT,
            home BOOLEAN,
            starter BOOLEAN,
            minutes INT,
            stats STRUCT(
                stl INT,
                blk INT,
                fgm INT,
                fga INT,
                fg3m INT,
                fg3a INT,
                fg2a INT,
                fg2m INT,
                tov INT,
                ast INT,
                fta INT,
                ftm INT,
                drb INT,
                orb INT
            ),
            shots STRUCT(
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
                miss_3pt INT
            ),
            assists STRUCT(
                dunks INT,
                layups INT,
                midrange INT,
                threes INT
            ),
            ez DOUBLE,
            ez_components STRUCT(
                rebounding DOUBLE,
                scoring DOUBLE,
                passing DOUBLE,
                stocks DOUBLE
            ),
            PRIMARY KEY (game_id, player_id)            
        );
    """

def insert_table_stage_top_lines(date: str) -> str:
    return f"""
    insert or ignore into stage_top_lines
    with lines as (
            select
                player_id,
                name,
                game_id,
                date,
                opp_id,
                team_id,
                home,
                starter,
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
        ),
        players as (
            select
                id,
                experience_years as years
            from stage_players
        ),
        team_adj as (
        pivot
            (select
                team_id,
                stat,
                ortg,
                drtg
            from stage_box_stat_adjustment_factors
            union all
            select
                team_id,
                stat,
                ortg,
                drtg
            from stage_shot_type_adjustment_factors)
        on stat using any_value(ortg) as ortg, any_value(drtg) as drtg
        group by team_id
        ),
        opp_adj as (
            pivot
            (select
                team_id,
                stat,
                ortg,
                drtg
            from stage_box_stat_adjustment_factors
            union all
            select
                team_id,
                stat,
                ortg,
                drtg
            from stage_shot_type_adjustment_factors)
        on stat using any_value(ortg) as ortg, any_value(drtg) as drtg
        group by team_id
        )
        select
            l.player_id,
            name,
            years,
            l.game_id,
            l.date,
            l.team_id,
            l.opp_id,
            l.home,
            starter,
            l.minutes,
            struct_pack(
                stl,
                blk,
                fgm,
                fga,
                fg3m,
                fg3a,
                fg2a:=fga-fg3a,
                fg2m:=fgm-fg3m,
                tov,
                ast,
                fta,
                ftm,
                drb,
                orb
            ) as stats,
            struct_pack(
                ast_tip,
                unast_tip,
                miss_tip,
                ast_dunk,
                unast_dunk,
                miss_dunk,
                ast_layup,
                unast_layup,
                miss_layup,
                ast_mid,
                unast_mid,
                miss_mid,
                ast_3pt,
                unast_3pt,
                miss_3pt
            ) as shots,
            struct_pack(
                dunks:=ast_to_dunk,
                layups:=ast_to_layup,
                midrange:=ast_to_mid,
                threes:=ast_to_3pt
            ) as assists,
            round(
                0.6*(orb/(ta.orb_ortg*oa.orb_drtg)) 
                + 0.3*(drb/(ta.drb_drtg*oa.drb_ortg)) 
                + 1.0*(stl/(ta.stl_drtg*oa.stl_ortg))
                + 0.7*(blk/(ta.blk_drtg*oa.blk_ortg)) 
                + 0.3*(ast_dunk/(ta.ast_dunk_ortg*oa.ast_dunk_drtg) 
                    + ast_mid/(ta.ast_mid_ortg*oa.ast_mid_drtg) 
                    + ast_layup/(ta.ast_layup_ortg*oa.ast_layup_drtg)) 
                + 1.0*(unast_dunk/(ta.unast_dunk_ortg*oa.unast_dunk_drtg) 
                    + unast_layup/(ta.unast_layup_ortg*oa.unast_layup_drtg) 
                    + unast_mid/(ta.unast_mid_ortg*oa.unast_mid_drtg)
                    + unast_tip) 
                + 2.0*unast_3pt/(ta.unast_3pt_ortg*oa.unast_3pt_drtg) 
                + 1.3*ast_3pt /(ta.ast_3pt_ortg*oa.ast_3pt_drtg)
                + (-0.7)*(miss_tip  
                    + miss_dunk/(ta.miss_dunk_ortg*oa.miss_dunk_drtg)
                    + miss_layup/(ta.miss_layup_ortg*oa.miss_layup_drtg)
                    + miss_dunk/(ta.miss_mid_ortg*oa.miss_mid_drtg)
                    + miss_3pt/(ta.miss_3pt_ortg*oa.miss_3pt_drtg)) 
                + 0.5*ftm
                - 0.5*(fta-ftm) 
                + 0.1*fta / (ta.fta_ortg*oa.fta_drtg)
                + 0.1*(fga/(ta.fga_ortg*oa.fga_drtg) 
                    - fg3a/(ta.fg3a_ortg*oa.fg3a_drtg)) 
                + 0.2*fg3a / (ta.fg3a_ortg*oa.fg3a_drtg)
                + 0.7*ast / (ta.ast_ortg*oa.ast_drtg)
                - 0.8*(tov/(ta.tov_ortg*oa.tov_drtg))
            , 3) as ez,
            struct_pack(
                rebounding:=0.6*(orb/(ta.orb_ortg*oa.orb_drtg)) 
                + 0.3*(drb/(ta.drb_drtg*oa.drb_ortg)),
                scoring:=0.3*(ast_dunk/(ta.ast_dunk_ortg*oa.ast_dunk_drtg) 
                    + ast_mid/(ta.ast_mid_ortg*oa.ast_mid_drtg) 
                    + ast_layup/(ta.ast_layup_ortg*oa.ast_layup_drtg)) 
                + 1.0*(unast_dunk/(ta.unast_dunk_ortg*oa.unast_dunk_drtg) 
                    + unast_layup/(ta.unast_layup_ortg*oa.unast_layup_drtg) 
                    + unast_mid/(ta.unast_mid_ortg*oa.unast_mid_drtg)
                    + unast_tip) 
                + 2.0*unast_3pt/(ta.unast_3pt_ortg*oa.unast_3pt_drtg) 
                + 1.3*ast_3pt /(ta.ast_3pt_ortg*oa.ast_3pt_drtg)
                + (-0.7)*(miss_tip  
                    + miss_dunk/(ta.miss_dunk_ortg*oa.miss_dunk_drtg)
                    + miss_layup/(ta.miss_layup_ortg*oa.miss_layup_drtg)
                    + miss_dunk/(ta.miss_mid_ortg*oa.miss_mid_drtg)
                    + miss_3pt/(ta.miss_3pt_ortg*oa.miss_3pt_drtg)) 
                + 0.5*ftm
                - 0.5*(fta-ftm) 
                + 0.1*fta / (ta.fta_ortg*oa.fta_drtg)
                + 0.1*(fga/(ta.fga_ortg*oa.fga_drtg) 
                    - fg3a/(ta.fg3a_ortg*oa.fg3a_drtg)) 
                + 0.2*fg3a / (ta.fg3a_ortg*oa.fg3a_drtg),
                passing:=0.7*ast / (ta.ast_ortg*oa.ast_drtg)
                - 0.8*(tov/(ta.tov_ortg*oa.tov_drtg)),
                stocks:=1.0*(stl/(ta.stl_drtg*oa.stl_ortg))
                + 0.7*(blk/(ta.blk_drtg*oa.blk_ortg))
            ) as ez_components
        from lines l join stage_player_shots_by_game s on l.game_id=s.game_id
        and l.player_id=s.player_id 
        join stage_player_assists_by_game a on l.game_id=a.game_id
        and l.player_id=a.player_id
        join team_adj ta on l.team_id=ta.team_id
        join opp_adj oa on l.opp_id=oa.team_id
        join players p on p.id=l.player_id        
        where l.date='{date}'
        order by ez desc
        returning player_id, name, years, game_id, ez;
    """

def top_lines_report_query(start_date:str, end_date:str, exp: list[int], top_n: int) -> str:
    """
    build the top lines html report query
    """
    return f"""
    with games as (
        from stage_top_lines
        where years in ({', '.join(map(str, exp))})
        and date between '{start_date}' and '{end_date}'
        order by ez desc
        limit {top_n}
    ),
    metrics as (
        select 
            player_id,
            game_id,
            count(game_id) over (partition by player_id) as games,
            avg(ez) over (partition by player_id) as mu,
            stddev(ez) over (partition by player_id) as sigma,
            max(ez) over (partition by player_id) as inf,
            median(ez) over (partition by player_id) as med,
            rank() over (partition by player_id order by ez desc) as game_rank
        from stage_top_lines
    )
    select
        rank() over (order by ez desc)        as class_rank,
        g.player_id,
        g.game_id,
        games,
        case when home then team_2_logo else team_1_logo end as team_logo,
        case when home then team_2_location else team_1_location end as team_location,
        case when home then team_1_logo else team_2_logo end as opp_logo,
        case when home then team_1_location else team_2_location end as opp_location,
        case when home then team_2_pts else team_1_pts end as team_pts,
        case when home then team_1_pts else team_2_pts end as opp_pts,
        t2.shortConferenceName as opp_conf,
        headshot_href,
        display_name,
        jersey,
        experience_abbreviation,
        position_abbreviation,
        display_height, 
        display_weight,
        t1.shortConferenceName as team_conf,
        g.date,
        home,
        starter,
        concat('<img width=150 src=\"', headshot_href, '\">', '<br>',
            display_name, ' #', jersey, '<br>',
            experience_abbreviation, ' ', position_abbreviation, ' ', display_height, ' ', display_weight) as bio,
        concat('<img width=72 src=\"', case when home then team_2_logo else team_1_logo end, '\">', '<br>',
            case when home then team_2_location else team_1_location end, '-', 
            case when home then team_2_pts else team_1_pts end, '<br>',
            t1.shortConferenceName) as team,
        concat('<img width=72 src=\"', case when home then team_1_logo else team_2_logo end, '\">', '<br>',
            case when home then team_1_location else team_2_location end, '-',
            case when home then team_1_pts else team_2_pts end, '<br>',
            t2.shortConferenceName) as opp,
        concat(g.minutes, ' min', '<br>', cast(g.minutes*poss/sgl.minutes as INT), ' poss') as game,
        g.minutes,
        cast(g.minutes*poss/sgl.minutes as INT) as poss,
        struct_pack(
            pts:=stats.ftm + 2*stats.fgm + stats.fg3m,
            ts:=cast(100*(stats.ftm + 2*stats.fgm + stats.fg3m)/nullif(2*stats.fga + (0.44*stats.fta),0) as INT),
            usg:=case when home 
                then cast(100.0 * (stats.fga + 0.44 * stats.fta + stats.tov) * g.minutes /
                                    (sgl.minutes * (team_2_stats.fga + 0.44 * team_2_stats.fta + team_2_stats.tov)) as INT)
                else cast(100.0 * (stats.fga + 0.44 * stats.fta + stats.tov) * g.minutes /
                                    (sgl.minutes * (team_1_stats.fga + 0.44 * team_1_stats.fta + team_1_stats.tov)) as INT)
                end,
            ppp:=round((stats.ftm + 2*stats.fgm + stats.fg3m)/nullif(stats.fga + 0.44 * stats.fta + stats.tov,0),2)
        ) as usg_struct,
        concat(
            stats.ftm + 2*stats.fgm + stats.fg3m, ' pts', '<br>',
            cast(100*(stats.ftm + 2*stats.fgm + stats.fg3m)/nullif(2*stats.fga + (0.44*stats.fta),0) as INT), ' ts', '<br>',
            case when home 
            then cast(100.0 * (stats.fga + 0.44 * stats.fta + stats.tov) * g.minutes /
                                (sgl.minutes * (team_2_stats.fga + 0.44 * team_2_stats.fta + team_2_stats.tov)) as INT)
            else cast(100.0 * (stats.fga + 0.44 * stats.fta + stats.tov) * g.minutes /
                                (sgl.minutes * (team_1_stats.fga + 0.44 * team_1_stats.fta + team_1_stats.tov)) as INT)
            end,
            ' usg','<br>',
            round((stats.ftm + 2*stats.fgm + stats.fg3m)/nullif(stats.fga + 0.44 * stats.fta + stats.tov,0),2), ' ppp'

        ) as usg, 
        round((stats.ftm + 2*stats.fgm + stats.fg3m)/nullif(stats.fga + 0.44 * stats.fta + stats.tov,0),2) as ppp,
        struct_pack(
            twos:= concat(stats.fgm-stats.fg3m, '-', stats.fga-stats.fg3a, ' (', shots.unast_dunk+shots.unast_layup+shots.unast_mid, ')'), 
            dunks:= concat(shots.ast_dunk+shots.unast_dunk, '-',shots.ast_dunk+shots.unast_dunk+shots.miss_dunk, ' (', shots.unast_dunk, ')'), 
            layups:= concat(shots.ast_layup+shots.unast_layup, '-',shots.ast_layup+shots.unast_layup+shots.miss_layup, ' (', shots.unast_layup, ')'),
            midrange:= concat(shots.ast_mid+shots.unast_mid, '-', shots.ast_mid+shots.unast_mid+shots.miss_mid, ' (', shots.unast_mid, ')'), 
            threes:= concat(stats.fg3m, '-', stats.fg3a, ' (', shots.unast_3pt, ')'),
            fts:= concat(stats.ftm, '-', stats.fta)
        ) as shots_struct,
        concat(
            stats.ast, ' ast', '<br>',
            stats.tov, ' tov', '<br>',
            stats.stl, ' stl', '<br>',
            stats.blk, ' blk', '<br>',
            stats.orb, ' orb', '<br>',
            stats.drb, ' drb', '<br>'
        ) as box,
        stats,
        concat(
            'ez: ', 
            case
                   when ez > mu + 2.0 * sigma then round(cast(ez as numeric),1) 
                   when ez > mu + 1.0 * sigma then round(cast(ez as numeric),1)
                   when ez < mu - 1.0 * sigma then round(cast(ez as numeric),1)
                   when ez < mu - 2.0 * sigma then round(cast(ez as numeric),1) 
                   else round(cast(ez as numeric),1)
            end, '<br>', 
            'ez75: ', round(75*ez*sgl.minutes/(g.minutes*poss), 1), '<br>',
            'avg: ',round(cast(mu as numeric), 1),'<br>',
            'std: ',round(cast(sigma as numeric), 1), '<br>',
            'max: ', round(cast(inf as numeric), 1), '<br>',
            'med: ', round(cast(med as numeric), 1)
        ) as ez,
        struct_pack(
            ez:=case
                   when ez > mu + 2.0 * sigma then round(cast(ez as double),1) 
                   when ez > mu + 1.0 * sigma then round(cast(ez as double),1)
                   when ez < mu - 1.0 * sigma then round(cast(ez as double),1)
                   when ez < mu - 2.0 * sigma then round(cast(ez as double),1) 
                   else round(cast(ez as double),1)
                end,
            ez75:=round(75*ez*sgl.minutes/(g.minutes*poss), 1),
            avg:=round(cast(mu as double), 1),
            std:=round(cast(sigma as double), 1),
            max:=round(cast(inf as double), 1),
            med:=round(cast(med as double), 1)
        ) as ez_struct,
        ez_components,
        assists,
        game_rank,
        case
            when ez > mu + 2.0 * sigma then '▲▲'
            when ez > mu + 1.0 * sigma then '▲'
            when ez < mu - 1.0 * sigma then '▼'
            when ez < mu - 2.0 * sigma then '▼▼'
            else ''
        end as notable
    from games g join metrics m on g.player_id=m.player_id and g.game_id=m.game_id
    join stage_game_logs sgl on g.game_id=sgl.game_id
    left join stage_players p on g.player_id=p.id
    left join stage_rsci_rankings rsci on p.full_name=rsci.Player
    join stage_teams t1 on g.team_id=t1.id
    join stage_teams t2 on g.opp_id=t2.id;
    """