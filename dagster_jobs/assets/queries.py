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

def create_table_stage_conferences(women: bool=False) -> str:
    """
    create the conferences table
    """

    create_query = lambda women: f"""
        create table if not exists {'stage_conferences_women' if women else 'stage_conferences'} (
            id INTEGER PRIMARY KEY,
            name STRING,
            shortName STRING,
            logo STRING
        );
    """

    query = create_query(women)
    return query

def create_table_stage_players(women:bool=False) -> str:
    """
    create players staging table
    """
    return f"""
    create table if not exists {'stage_players_women' if women else 'stage_players'} (
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

def create_table_stage_teams(women: bool = False) -> str:
    """
    create the teams table
    """
    generate_query = lambda women: f"""
    create table if not exists {'stage_teams_women' if women else 'stage_teams'} (
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
    query = generate_query(women)
    return query

def insert_table_stage_conferences(path: str, women:bool=False) -> str:
    """
    insert conference data into staging table
    """

    insert_query = lambda women: f"""
    insert or ignore into {'stage_conferences_women' if women else 'stage_conferences'}
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

    query = insert_query(women)
    return query

def insert_table_stage_players(path: list[str], women: bool=False) -> str:
    """
    insert players into staging table
    """
    return f"""
    insert or ignore into {'stage_players_women' if women else 'stage_players'}
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

def insert_table_stage_teams(path: str, women: bool=False) -> str:
    """
    insert teams data into staging table
    """
    generate_query = lambda women: f"""
    insert or ignore into {'stage_teams_women' if women else 'stage_teams'}
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
    query = generate_query(women)
    return query

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

def create_table_stage_daily_scoreboard(women:bool=False) -> str: 
    """
    create staging table for daily scoreboard
    """
    return f"""
    create table if not exists {'stage_daily_scoreboard_women' if women else 'stage_daily_scoreboard'} (
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

def insert_table_stage_daily_scoreboard(path:str, date: str, women: bool=False) -> str:
    """
    transform scoreboard data from files and load into staging table
    """
    return f"""
    insert or ignore into {'stage_daily_scoreboard_women' if women else 'stage_daily_scoreboard'} 
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

def fetch_completed_game_ids(date:str, women: bool=False) -> str:
    """
    return game_ids from staging table
    """
    return f"""
        select 
            game_id
        from {'stage_daily_scoreboard_women' if women else 'stage_daily_scoreboard'}
        where (date = '{date}') 
            and (completed is true);
    """

def create_table_stage_game_logs(women: bool=False) -> str: 
    """
    create staging table for game logs
    """
    return f"""
    create table if not exists {'stage_game_logs_women' if women else 'stage_game_logs'} (
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

def insert_table_stage_game_logs(files:list[str], women: bool=False) -> str:
    """
    insert box score totals into staging table
    """
    return f"""
    insert or ignore into {'stage_game_logs_women' if women else 'stage_game_logs'}
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
        from s join {'stage_daily_scoreboard_women' if women else 'stage_daily_scoreboard'} sds on s.game_id=sds.game_id
        returning game_id;
    """

def create_table_stage_player_lines(women: bool=False) -> str:
    """
    staging table for player lines (eg box score totals)
    """

    return f"""
        create table if not exists {'stage_player_lines_women' if women else 'stage_player_lines'} (
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
            stats STRUCT(pts INTEGER, fgm INTEGER, fga INTEGER, fg3m INTEGER, fg3a INTEGER, ftm INTEGER, fta INTEGER, reb INTEGER, ast INTEGER, tov INTEGER, stl INTEGER, blk INTEGER, orb INTEGER, drb INTEGER, pf INTEGER, "minutes" INTEGER),
            PRIMARY KEY (game_id, player_id)
        )
    """

def insert_table_stage_player_lines(files: list[str], date: str, women: bool=False) -> str:
    """
    insert data into staging table for player lines
    """
    
    return f"""
    insert or ignore into {'stage_player_lines_women' if women else 'stage_player_lines'}
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
            -- Handle two different stats formats: standard (PTS first) vs alternative (MIN first)
            struct_pack(
                minutes := case 
                    when stat_format = 'alt' then NULLIF(stats[1], '--')::INT
                    else NULLIF(stats[13], '--')::INT
                end,
                pts := case 
                    when stat_format = 'alt' then stats[2]::INT
                    else stats[1]::INT
                end,
                orb := case 
                    when stat_format = 'alt' then stats[11]::INT
                    else stats[10]::INT
                end,
                drb := case 
                    when stat_format = 'alt' then stats[12]::INT
                    else stats[11]::INT
                end,
                reb := case 
                    when stat_format = 'alt' then stats[6]::INT
                    else stats[5]::INT
                end,
                ast := case 
                    when stat_format = 'alt' then stats[7]::INT
                    else stats[6]::INT
                end,
                stl := case 
                    when stat_format = 'alt' then stats[9]::INT
                    else stats[8]::INT
                end,
                blk := case 
                    when stat_format = 'alt' then stats[10]::INT
                    else stats[9]::INT
                end,
                tov := case 
                    when stat_format = 'alt' then stats[8]::INT
                    else stats[7]::INT
                end,
                fgm := case 
                    when stat_format = 'alt' then regexp_extract(stats[3], '([0-9]+)-',1)::INT
                    else regexp_extract(stats[2], '([0-9]+)-',1)::INT
                end,
                fga := case 
                    when stat_format = 'alt' then regexp_extract(stats[3], '-([0-9]+)',1)::INT
                    else regexp_extract(stats[2], '-([0-9]+)',1)::INT
                end,
                fg3m := case 
                    when stat_format = 'alt' then regexp_extract(stats[4], '([0-9]+)-',1)::INT
                    else regexp_extract(stats[3], '([0-9]+)-',1)::INT
                end,
                fg3a := case 
                    when stat_format = 'alt' then regexp_extract(stats[4], '-([0-9]+)',1)::INT
                    else regexp_extract(stats[3], '-([0-9]+)',1)::INT
                end,
                ftm := case 
                    when stat_format = 'alt' then regexp_extract(stats[5], '([0-9]+)-',1)::INT
                    else regexp_extract(stats[4], '([0-9]+)-',1)::INT
                end,
                fta := case 
                    when stat_format = 'alt' then regexp_extract(stats[5], '-([0-9]+)',1)::INT
                    else regexp_extract(stats[4], '-([0-9]+)',1)::INT
                end,
                pf := case 
                    when stat_format = 'alt' then stats[13]::INT
                    else stats[12]::INT
                end
            ) as stats
        from
        (select 
            game_id,
            season,
            team_1_id,
            team_2_id,
            players.displayOrder,
            -- Detect stats format: 'alt' if labels[1]='MIN', 'std' if labels[1]='PTS'
            case when players.statistics[1].labels[1] = 'MIN' then 'alt' else 'std' end as stat_format,
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
        where didNotPlay is not true and stats[1] is not null
        returning player_id;
    """

def create_table_stage_plays(women: bool=False) -> str:
    """"
    create table for staging play-by-play
    """
    return f"""
    create table if not exists {'stage_plays_women' if women else 'stage_plays'} (
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

def insert_table_stage_plays(files:list[str], date:str, women: bool=False) -> str:
    """
    insert plays into staging table
    """
    return f"""
    insert or ignore into {'stage_plays_women' if women else 'stage_plays'}
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

def create_table_stage_player_shots_by_game(women: bool=False) -> str:
    """
    for tracking shot types (dunks, layups, etc)
    """
    return f"""
    create table if not exists {'stage_player_shots_by_game_women' if women else 'stage_player_shots_by_game'} (
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

def insert_table_stage_player_shots_by_game(date: str, women: bool=False) -> str:
    """
    count dunks, layups, mid-range and 3pt shots by player by game
    """
    return f"""
    insert into {'stage_player_shots_by_game_women' if women else 'stage_player_shots_by_game'}
    select 
        game_id,
        any_value(date) as date,
        any_value(team_id) as team_id,
        any_value(opp_id) as opp_id,
        any_value(home) as home,
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
    from {'stage_plays_women' if women else 'stage_plays'}
    where type_id in (437, 558, 572, 574) and
    date='{date}' and player_1_id is not null
    group by game_id, player_1_id
    returning game_id, team_id, opp_id, home, player_id;
    """

def create_table_stage_player_assists_by_game(women: bool=False) -> str:
    """
    create table for staging player assist types
    """
    return f"""
    create table if not exists {'stage_player_assists_by_game_women' if women else 'stage_player_assists_by_game'} (
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

def stage_player_assists_by_game(date: str, women: bool=False) -> str:
    """
    count different types of assists
    """
    return f"""
    insert or ignore into {'stage_player_assists_by_game_women' if women else 'stage_player_assists_by_game'}
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
    from {'stage_plays_women' if women else 'stage_plays'}
    where type_id in (558, 572, 574) and
    date='{date}'
    and player_2_id is not null
    group by ALL
    returning game_id, team_id, player_id, ast_to_dunk, ast_to_layup, ast_to_mid, ast_to_3pt;
    """

def create_table_stage_top_lines(women: bool=False) -> str:
    """
    table for storing daily player stats that will be used to create Top Lines reports
    """
    return f"""
        create table if not exists {'stage_top_lines_women' if women else 'stage_top_lines'} (
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

def insert_table_stage_top_lines(date: str, women: bool=False) -> str:
    return f"""
    insert or ignore into {"stage_top_lines_women" if women else "stage_top_lines"}
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
            from {'stage_player_lines_women' if women else 'stage_player_lines'}
        ),
        players as (
            select
                id,
                experience_years as years
            from {'stage_players_women' if women else 'stage_players'}
        ),
        team_adj as (
        pivot
            (select
                team_id,
                stat,
                coalesce(ortg,1.0) as ortg,
                coalesce(drtg,1.0) as drtg
            from {'stage_box_stat_adjustment_factors_women' if women else 'stage_box_stat_adjustment_factors'}
            union all
            select
                team_id,
                stat,
                coalesce(ortg,1.0) as ortg,
                coalesce(drtg,1.0) as drtg
            from {'stage_shot_type_adjustment_factors_women' if women else 'stage_shot_type_adjustment_factors'})
        on stat using any_value(ortg) as ortg, any_value(drtg) as drtg
        group by team_id
        ),
        opp_adj as (
            pivot
            (select
                team_id,
                stat,
                coalesce(ortg,1.0) as ortg,
                coalesce(drtg,1.0) as drtg
            from {'stage_box_stat_adjustment_factors_women' if women else 'stage_box_stat_adjustment_factors'}
            union all
            select
                team_id,
                stat,
                coalesce(ortg,1.0) as ortg,
                coalesce(drtg,1.0) as drtg
            from {'stage_shot_type_adjustment_factors_women' if women else 'stage_shot_type_adjustment_factors'})
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
                    + miss_mid/(ta.miss_mid_ortg*oa.miss_mid_drtg)
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
                    + miss_mid/(ta.miss_mid_ortg*oa.miss_mid_drtg)
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
        from lines l join {'stage_player_shots_by_game_women' if women else 'stage_player_shots_by_game'} s on l.game_id=s.game_id
        and l.player_id=s.player_id 
        left join {'stage_player_assists_by_game_women' if women else 'stage_player_assists_by_game'} a on l.game_id=a.game_id
        and l.player_id=a.player_id
        join team_adj ta on l.team_id=ta.team_id
        join opp_adj oa on l.opp_id=oa.team_id
        join players p on p.id=l.player_id        
        where l.date='{date}'
        order by ez desc
        returning player_id, name, years, game_id, ez;
    """

def top_lines_report_query(start_date:str, end_date:str, exp: list[int], top_n: int, women: bool=False) -> str:
    """
    build the top lines html report query
    """
    return f"""
    with games as (
        from {'stage_top_lines_women' if women else 'stage_top_lines'}
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
        from {'stage_top_lines_women' if women else 'stage_top_lines'}
    )
    select
        rank() over (partition by experience_abbreviation order by ez desc)        as class_rank,
        g.player_id,
        g.game_id,
        games,
        {"recruiting.rank" if women else "RSCI"} as recruit_rank,
        DATE '2026-06-25' - birthday::DATE as age_at_draft,
        tr.rank as team_rank,
        tro.rank as opp_rank,
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
        p.city,
        state,
        country,
        experience_abbreviation,
        experience_display_value,
        position_abbreviation,
        position_display_name,
        display_height, 
        display_weight,
        t1.shortConferenceName as team_conf,
        g.date,
        home,
        starter,
        g.minutes,
        cast(g.minutes*poss/sgl.minutes as INT) as poss,
        struct_pack(
            pts:=stats.ftm + 2*stats.fgm + stats.fg3m,
            ts:=cast(100*(stats.ftm + 2*stats.fgm + stats.fg3m)/nullif(2*(stats.fga + 0.44*stats.fta),0) as INT),
            tspctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (stats.ftm + 2*stats.fgm + stats.fg3m + 0.001)/nullif(2*(stats.fga + 0.44*stats.fta),0) asc)),
            orbpct:=case when home 
                then cast(100.0 * stats.orb * sgl.minutes /
                                    (g.minutes * (team_2_stats.drb + team_1_stats.orb)) as INT)
                else cast(100.0 * stats.orb * sgl.minutes /
                                    (g.minutes * (team_1_stats.drb + team_2_stats.orb)) as INT)
                end,
            orbpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                case when home 
                then cast(100.0 * (stats.orb+0.001) * sgl.minutes /
                                    (g.minutes * (team_2_stats.drb + team_1_stats.orb)) as INT)
                else cast(100.0 * (stats.orb+0.001) * sgl.minutes /
                                    (g.minutes * (team_1_stats.drb + team_2_stats.orb)) as INT)
                end asc)),
            drbpct:=case when home 
                then cast(100.0 * stats.drb * sgl.minutes /
                                    (g.minutes * (team_2_stats.orb + team_1_stats.drb)) as INT)
                else cast(100.0 * stats.drb * sgl.minutes /
                                    (g.minutes * (team_1_stats.orb + team_2_stats.drb)) as INT)
                end,
            drbpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                case when home 
                then cast(100.0 * (stats.drb+0.001) * sgl.minutes /
                                    (g.minutes * (team_2_stats.orb + team_1_stats.drb)) as INT)
                else cast(100.0 * (stats.drb+0.001) * sgl.minutes /
                                    (g.minutes * (team_1_stats.orb + team_2_stats.drb)) as INT)
                end asc)),
            usg:=case when home 
                then cast(100.0 * (stats.fga + 0.44 * stats.fta + stats.tov) * sgl.minutes /
                                    (g.minutes * (team_2_stats.fga + 0.44 * team_2_stats.fta + team_2_stats.tov)) as INT)
                else cast(100.0 * (stats.fga + 0.44 * stats.fta + stats.tov) * sgl.minutes /
                                    (g.minutes * (team_1_stats.fga + 0.44 * team_1_stats.fta + team_1_stats.tov)) as INT)
                end,
            usgpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                case when home 
                then cast(100.0 * (stats.fga + 0.44 * stats.fta + stats.tov + 0.001) * sgl.minutes /
                                    (g.minutes * (team_2_stats.fga + 0.44 * team_2_stats.fta + team_2_stats.tov)) as INT)
                else cast(100.0 * (stats.fga + 0.44 * stats.fta + stats.tov + 0.001) * sgl.minutes /
                                    (g.minutes * (team_1_stats.fga + 0.44 * team_1_stats.fta + team_1_stats.tov)) as INT)
                end asc)),
            ppp:=round((stats.ftm + 2*stats.fgm + stats.fg3m)/nullif(stats.fga + 0.44 * stats.fta + stats.tov,0),2),
            ppppctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (stats.ftm + 2*stats.fgm + stats.fg3m + 0.001)/nullif(stats.fga + 0.44 * stats.fta + stats.tov,0) asc)),
            astpct:=case when home 
                then cast(100.0 * stats.ast * sgl.minutes /
                                    (g.minutes * (team_2_stats.fgm - stats.fgm)) as INT)
                else cast(100.0 * stats.ast * sgl.minutes /
                                    (g.minutes * (team_1_stats.fgm - stats.fgm)) as INT)
                end,
            astpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                case when home 
                then cast(100.0 * (stats.ast+0.001) * sgl.minutes /
                                    (g.minutes * (team_2_stats.fgm - stats.fgm)) as INT)
                else cast(100.0 * (stats.ast+0.001) * sgl.minutes /
                                    (g.minutes * (team_1_stats.fgm - stats.fgm)) as INT)
                end asc)),
            tovpct:=cast(100.0 * stats.tov/(stats.fga + 0.44*stats.fta + stats.tov) as INT),
            tovpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((stats.tov+0.001)/(stats.fga + 0.44*stats.fta + stats.tov)) desc)),
            ftr:=round(stats.fta/stats.fga, 2),
            ftrpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((stats.fta + 0.001)/stats.fga) asc))
        ) as usg_struct,
        struct_pack(
            twos:= concat(stats.fg2m, '-', stats.fg2a,'&nbsp;',shots.unast_dunk+shots.unast_layup+shots.unast_mid, 'u'), 
            twospctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((stats.fg2a + 0.001)/stats.fga) asc)),
            dunks:= concat(shots.ast_dunk+shots.unast_dunk, '-',shots.ast_dunk+shots.unast_dunk+shots.miss_dunk, '&nbsp;', shots.unast_dunk, 'u'), 
            dunkspctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((shots.ast_dunk+shots.unast_dunk+shots.miss_dunk + 0.001)/stats.fga) asc)),
            layups:= concat(shots.ast_layup+shots.unast_layup, '-',shots.ast_layup+shots.unast_layup+shots.miss_layup, '&nbsp;',shots.unast_layup, 'u'),
            layupspctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((shots.ast_layup+shots.unast_layup+shots.miss_layup + 0.001)/stats.fga) asc)),
            midrange:= concat(shots.ast_mid+shots.unast_mid, '-', shots.ast_mid+shots.unast_mid+shots.miss_mid, '&nbsp;',shots.unast_mid, 'u'), 
            midrangepctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((shots.ast_mid+shots.unast_mid+shots.miss_mid + 0.001)/stats.fga) asc)),
            threes:= concat(stats.fg3m, '-', stats.fg3a, '&nbsp;',shots.unast_3pt, 'u'),
            threespctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((stats.fg3a + 0.001)/stats.fga) asc)),
            fts:= concat(stats.ftm, '-', stats.fta)
        ) as shots_struct,
        stats,
        struct_pack(
            pts:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((stats.ftm + 2*stats.fgm + stats.fg3m)*sgl.minutes/(g.minutes*poss)) asc)),
            ast:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (stats.ast*sgl.minutes/(g.minutes*poss)) asc)),
            tov:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (stats.tov*sgl.minutes/(g.minutes*poss)) desc)),
            orb:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (stats.orb*sgl.minutes/(g.minutes*poss)) asc)),
            drb:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (stats.drb*sgl.minutes/(g.minutes*poss)) asc)),
            reb:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((stats.orb+stats.drb)*sgl.minutes/(g.minutes*poss)) asc)),
            stl:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (stats.stl*sgl.minutes/(g.minutes*poss)) asc)),
            blk:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (stats.blk*sgl.minutes/(g.minutes*poss)) asc))
        ) as percentiles,
        struct_pack(
            ez:=round(cast(ez as double),1),
            ezpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ez asc)),
            ez75:=round(75*ez*sgl.minutes/(g.minutes*poss), 1),
            ez75pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (ez*sgl.minutes/(g.minutes*poss)) asc)),
            avg:=round(cast(mu as double), 1),
            avgpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                mu asc)),
            std:=round(cast(sigma as double), 1),
            stdpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                sigma asc)),
            max:=round(cast(inf as double), 1),
            maxpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                inf asc)),
            med:=round(cast(med as double), 1),
            medpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                med asc))
        ) as ez_struct,
        ez_components,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            ez_components.scoring asc)) as scoringpctile,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            ez_components.rebounding asc)) as reboundingpctile,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            ez_components.passing asc)) as passingpctile,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            ez_components.stocks asc)) as stockspctile,
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
    join {'stage_game_logs_women' if women else 'stage_game_logs'} sgl on g.game_id=sgl.game_id
    left join {'stage_players_women' if women else 'stage_players'} p on g.player_id=p.id
    left join {'stage_hoopgurlz_rankings' if women else 'stage_rsci_rankings'} recruiting on p.full_name=recruiting.{'name' if women else 'Player'}
    left join stage_prospect_birthdays b on p.full_name=b.name
    join {'stage_teams_women' if women else 'stage_teams'} t1 on g.team_id=t1.id
    join {'stage_teams_women' if women else 'stage_teams'} t2 on g.opp_id=t2.id
    join {'stage_team_ratings_women' if women else 'stage_team_ratings'} tr on g.team_id=tr.team_id
    join {'stage_team_ratings_women' if women else 'stage_team_ratings'} tro on g.opp_id=tro.team_id
    order by class_rank asc;
    """

def prospect_rankings_report_query(start_date:str, end_date:str, exp: list[int], top_n: int, women: bool=False) -> str:
    """
    build the top lines html report query
    """
    return f"""
    with stats as (
        select
            player_id,
            sum(case when starter then 1 else 0 end) as gs,
            sum(case when tl.minutes>0 then 1 else 0 end) as gp,
            sum(tl.minutes) as minutes,
            sum(g.poss) as team_poss,
            sum(g.minutes) as team_minutes,
            sum(case when home then team_2_stats.orb else team_1_stats.orb end) as team_orb,
            sum(case when home then team_2_stats.drb else team_1_stats.drb end) as team_drb,
            sum(case when home then team_1_stats.orb else team_2_stats.orb end) as opp_orb,
            sum(case when home then team_1_stats.drb else team_2_stats.drb end) as opp_drb,
            sum(case when home then team_2_stats.tov else team_1_stats.tov end) as team_tov,
            sum(case when home then team_2_stats.fga else team_1_stats.fga end) as team_fga,
            sum(case when home then team_2_stats.fgm else team_1_stats.fgm end) as team_fgm,
            sum(case when home then team_2_stats.fta else team_1_stats.fta end) as team_fta,
            sum(case when home then team_1_stats.fga else team_2_stats.fga end) as opp_fga,
            sum(case when home then team_1_stats.fg3a else team_2_stats.fg3a end) as opp_fg3a,
            sum(stats.stl) as stl,
            sum(stats.blk) as blk,
            sum(stats.fgm) as fgm,
            sum(stats.fga) as fga,
            sum(stats.fg3m) as fg3m,
            sum(stats.fg3a) as fg3a,
            sum(stats.fg2a) as fg2a,
            sum(stats.fg2m) as fg2m,
            sum(stats.tov) as tov,
            sum(stats.ast) ast,
            sum(stats.fta) as fta,
            sum(stats.ftm) as ftm,
            sum(stats.drb) as drb,
            sum(stats.orb) as orb,
            sum(shots.ast_tip) as ast_tip,
            sum(shots.unast_tip) as unast_tip,
            sum(shots.miss_tip) as miss_tip,
            sum(shots.ast_dunk) as ast_dunk,
            sum(shots.unast_dunk) as unast_dunk,
            sum(shots.miss_dunk) as miss_dunk,
            sum(shots.ast_layup) as ast_layup,
            sum(shots.unast_layup) as unast_layup,
            sum(shots.miss_layup) as miss_layup,
            sum(shots.ast_mid) as ast_mid,
            sum(shots.unast_mid) as unast_mid,
            sum(shots.miss_mid) as miss_mid,
            sum(shots.ast_3pt) as ast_3pt,
            sum(shots.unast_3pt) as unast_3pt,
            sum(shots.miss_3pt) as miss_3pt,
            sum(assists.dunks) as ast_to_dunk,
            sum(assists.layups) as ast_to_layup,
            sum(assists.midrange) as ast_to_mid,
            sum(assists.threes) as ast_to_3pt,
            sum(ez) as ez,
            sum(ez_components.rebounding) as ez_rebounding,
            sum(ez_components.scoring) as ez_scoring,
            sum(ez_components.passing) as ez_passing,
            sum(ez_components.stocks) as ez_defense
        from {'stage_top_lines_women' if women else 'stage_top_lines'} tl 
        join {'stage_game_logs_women' if women else 'stage_game_logs'} g on tl.game_id=g.game_id
        where years in ({', '.join(map(str, exp))})
        and tl.date between '{start_date}' and '{end_date}'
        group by player_id
        order by ez desc
        limit {top_n}
    ),
    sos as (
        with team_games as (
            with schedules as (
                with games as (
                select
                    team_1_id as team_id,
                    game_id
                from stage_game_logs
                where (team_1_pts is not null) and not ((team_1_logo is null) or (team_2_logo is null))
                union
                select
                    team_2_id as team_id,
                    game_id
                from stage_game_logs
                where (team_2_pts is not null) and not ((team_1_logo is null) or (team_2_logo is null))
                )
                select
                    team_id,
                    list(game_id) as games
                from games
                group by team_id
            )
            select
                team_id,
                unnest(games) as game_id
            from schedules
        )
        select
            team.team_id,
            rank() over (order by avg(ortg-drtg) desc) as avg
        from team_games team join team_games opp on team.game_id=opp.game_id
            and team.team_id<>opp.team_id
            join stage_teams teams on teams.id=team.team_id
            join stage_teams opps on opps.id=opp.team_id
            join stage_team_ratings tr on opp.team_id=tr.team_id
        group by ALL
    )
    select
        s.player_id,
        p.team_id,
        {'RSCI' if not women else 'rsci.rank'} as recruit_rank,
        DATE '2026-06-25' - birthday::DATE as age_at_draft,
        display_name,
        location as team_location,
        color,
        alternateColor as alternate_color,
        tr.rank as team_rank,
        sos.avg as sos,
        shortConferenceName as team_conf,
        experience_abbreviation,
        experience_display_value,
        position_abbreviation,
        position_display_name,
        headshot_href,
        display_height,
        display_weight,
        jersey,
        p.city,
        state,
        country,
        ee.agency,
        -- combine.height_no_shoes,
        --combine.standing_reach,
        --combine.weight_lbs,
        --combine.wingspan,
        ez,
        gs,
        gp,
        ez/gp as game_score,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            (minutes*team_poss/team_minutes) asc)) as posspctile,
        100.0*(percent_rank() over (partition by experience_abbreviation order by 
            (ez* team_minutes / (minutes * team_poss)) asc)) as ez_poss_pctile,
        100.0*(percent_rank() over (partition by experience_abbreviation order by 
            (ez/gp) asc)) as ez_game_pctile,
        minutes,
        team_minutes,
        team_poss,
        100.0*(percent_rank() over (partition by experience_abbreviation order by minutes/gp asc)) as mpgpctile,
        struct_pack(
            ez_rebounding,
            reb_pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by (ez_rebounding* team_minutes / (minutes * team_poss)) asc)),
            ez_scoring,
            score_pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by (ez_scoring* team_minutes / (minutes * team_poss)) asc)),
            ez_passing,
            pass_pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by (ez_passing* team_minutes / (minutes * team_poss)) asc)),
            ez_defense,
            def_pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by (ez_defense* team_minutes / (minutes * team_poss)) asc))
        ) as ez_struct,
        struct_pack(
            ast_tip,
            ast_mid,
            ast_layup,
            ast_dunk,
            ast_3pt,
            unast_tip,
            unast_mid,
            unast_layup,
            unast_dunk,
            unast_3pt,
            miss_tip,
            miss_mid,
            miss_layup,
            miss_dunk,
            miss_3pt,
            fta,
            ftm,
            fga,
            fgm,
            fg2a,
            fg2m
        ) as shots,
        ast,
        ast_to_dunk,
        ast_to_layup,
        ast_to_mid,
        ast_to_3pt,
        struct_pack(
            dunk_assists:=ast_to_dunk,
            layup_assists:=ast_to_layup,
            mid_assists:=ast_to_mid,
            three_assists:=ast_to_3pt,
            total_shot_assists:=ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt,
            dunk_pct:=case when (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt) > 0 
                then round(100.0 * ast_to_dunk / (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt), 1) 
                else 0.0 end,
            layup_pct:=case when (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt) > 0 
                then round(100.0 * ast_to_layup / (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt), 1) 
                else 0.0 end,
            mid_pct:=case when (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt) > 0 
                then round(100.0 * ast_to_mid / (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt), 1) 
                else 0.0 end,
            three_pct:=case when (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt) > 0 
                then round(100.0 * ast_to_3pt / (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt), 1) 
                else 0.0 end,
            dunk_assists_pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ast_to_dunk * team_minutes / (minutes * team_poss) asc)),
            layup_assists_pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ast_to_layup * team_minutes / (minutes * team_poss) asc)),
            mid_assists_pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ast_to_mid * team_minutes / (minutes * team_poss) asc)),
            three_assists_pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ast_to_3pt * team_minutes / (minutes * team_poss) asc)),
            dunk_pct_pctile:=case when (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt) = 0 then 0
                else 100.0*(percent_rank() over (partition by experience_abbreviation order by
                    ast_to_dunk / nullif(ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt, 0) asc))
                end,
            layup_pct_pctile:=case when (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt) = 0 then 0
                else 100.0*(percent_rank() over (partition by experience_abbreviation order by
                    ast_to_layup / nullif(ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt, 0) asc))
                end,
            mid_pct_pctile:=case when (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt) = 0 then 0
                else 100.0*(percent_rank() over (partition by experience_abbreviation order by
                    ast_to_mid / nullif(ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt, 0) asc))
                end,
            three_pct_pctile:=case when (ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt) = 0 then 0
                else 100.0*(percent_rank() over (partition by experience_abbreviation order by
                    ast_to_3pt / nullif(ast_to_dunk + ast_to_layup + ast_to_mid + ast_to_3pt, 0) asc))
                end
        ) as assist_distribution,
        tov,
        tov/gp as tpg,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            tov/gp desc)) as tpgpctile,
        stl,
        stl/gp as spg,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            stl/gp asc)) as spgpctile,
        blk,
        blk/gp as bpg,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            blk/gp asc)) as bpgpctile,
        orb,
        drb,
        (orb+drb)/gp as rpg,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            (orb+drb)/gp asc)) as rpgpctile,
        ast/gp as apg,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            ast/gp asc)) as apgpctile,
        (2*fg2m + 3*(fgm-fg2m) + ftm)/gp as ppg,
        100.0*(percent_rank() over (partition by experience_abbreviation order by
            (2*fg2m + 3*(fgm-fg2m) + ftm)/gp asc)) as ppgpctile,
        rank() over (partition by experience_abbreviation order by ez/gp desc) as class_rank,
        100.0*(percent_rank() over (partition by experience_abbreviation order by ez asc)) as ez_pctile,
        struct_pack(
            fg2pct:=100*fg2m/nullif(fg2a,0),
            fg2pctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                fg2m/nullif(fg2a,0) asc)),
            fg3pct:=100*fg3m/nullif(fg3a,0),
            fg3pctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                fg3m/nullif(fg3a,0) asc)),
            ftpct:=100*ftm/nullif(fta,0),
            ftpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ftm/nullif(fta,0) asc)),
            dunkpct:=100*(unast_dunk+ast_dunk)/nullif(unast_dunk+ast_dunk+miss_dunk,0),
            dunkpctpctile:=case when (unast_dunk+ast_dunk+miss_dunk)==0 then 0
            else 100.0*(percent_rank() over (partition by experience_abbreviation order by
                ((unast_dunk+ast_dunk)/nullif(unast_dunk+ast_dunk+miss_dunk,0)) asc))
                end,
            dunka100:=cast(100.0 * (unast_dunk+ast_dunk+miss_dunk) * team_minutes / (minutes * team_poss) as numeric),
            dunka100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (unast_dunk+ast_dunk+miss_dunk) * team_minutes / (minutes * team_poss) asc)),
            layuppct:=100*(unast_layup+ast_layup)/nullif(unast_layup+ast_layup+miss_layup,0),
            layuppctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (unast_layup+ast_layup)/nullif(unast_layup+ast_layup+miss_layup,0) asc)),
            layupa100:=cast(100.0 * (unast_layup+ast_layup+miss_layup) * team_minutes / (minutes * team_poss) as numeric),
            layupa100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (unast_layup+ast_layup+miss_layup) * team_minutes / (minutes * team_poss) asc)),
            midpct:=100*(unast_mid+ast_mid)/nullif(unast_mid+ast_mid+miss_mid,0),
            midpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (unast_mid+ast_mid)/nullif(unast_mid+ast_mid+miss_mid,0) asc)),
            mida100:=cast(100.0 * (unast_mid+ast_mid+miss_mid) * team_minutes / (minutes * team_poss) as numeric),
            mida100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (unast_mid+ast_mid+miss_mid) * team_minutes / (minutes * team_poss) asc)),
            tippct:=100*(unast_tip+ast_tip)/nullif(unast_tip+ast_tip+miss_tip,0),
            tippctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (unast_tip+ast_tip)/nullif(unast_tip+ast_tip+miss_tip,0) asc)),
            usg:=100.0 * (fga + 0.44 * fta + tov) * team_minutes / (minutes * team_poss),
            usgpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (fga + 0.44 * fta + tov) * team_minutes / (minutes * team_poss) asc)),
            ts:=cast(100*(ftm + 2*fgm + fg3m)/nullif(2*(fga + 0.44*fta),0) as INT),
            tspctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (ftm + 2*fgm + fg3m)/nullif(2*(fga + 0.44*fta),0) asc)),
            ppp:=(ftm + 2*fgm + fg3m)/nullif(fga + 0.44 * fta + tov,0),
            ppppctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                (ftm + 2*fgm + fg3m)/nullif(fga + 0.44 * fta + tov,0) asc)),
            ftr:=fta/nullif(fga, 0),
            ftrpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                fta/nullif(fga, 0) asc)),
            fta100:=cast(100.0 * fta * team_minutes / (minutes * team_poss) as numeric),
            fta100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                fta * team_minutes / (minutes * team_poss) asc)),
            fg2a100:=cast(100.0 * fg2a * team_minutes / (minutes * team_poss) as numeric),
            fg2a100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                fg2a * team_minutes / (minutes * team_poss) asc)),
            fg3a100:=cast(100.0 * fg3a * team_minutes / (minutes * team_poss) as numeric),
            fg3a100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                fg3a * team_minutes / (minutes * team_poss) asc))
        ) as shooting,
        struct_pack(
            dunk_ast_pct:=case when (ast_dunk + unast_dunk) > 0 
                then round(100.0 * ast_dunk / (ast_dunk + unast_dunk), 1) 
                else 0.0 end,
            layup_ast_pct:=case when (ast_layup + unast_layup) > 0 
                then round(100.0 * ast_layup / (ast_layup + unast_layup), 1) 
                else 0.0 end,
            mid_ast_pct:=case when (ast_mid + unast_mid) > 0 
                then round(100.0 * ast_mid / (ast_mid + unast_mid), 1) 
                else 0.0 end,
            three_ast_pct:=case when (ast_3pt + unast_3pt) > 0 
                then round(100.0 * ast_3pt / (ast_3pt + unast_3pt), 1) 
                else 0.0 end,
            dunk_ast_pct_pctile:=case when (ast_dunk + unast_dunk) = 0 then 0
                else 100.0*(percent_rank() over (partition by experience_abbreviation order by
                    ast_dunk / nullif(ast_dunk + unast_dunk, 0) asc))
                end,
            layup_ast_pct_pctile:=case when (ast_layup + unast_layup) = 0 then 0
                else 100.0*(percent_rank() over (partition by experience_abbreviation order by
                    ast_layup / nullif(ast_layup + unast_layup, 0) asc))
                end,
            mid_ast_pct_pctile:=case when (ast_mid + unast_mid) = 0 then 0
                else 100.0*(percent_rank() over (partition by experience_abbreviation order by
                    ast_mid / nullif(ast_mid + unast_mid, 0) asc))
                end,
            three_ast_pct_pctile:=case when (ast_3pt + unast_3pt) = 0 then 0
                else 100.0*(percent_rank() over (partition by experience_abbreviation order by
                    ast_3pt / nullif(ast_3pt + unast_3pt, 0) asc))
                end
        ) as assisted_shots,
        struct_pack(
            ast100:=round(cast(100.0 * ast * team_minutes / (minutes * team_poss) as numeric),1),
            ast100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ast * team_minutes / (minutes * team_poss) asc)),
            tov100:=round(cast(100.0 * tov * team_minutes / (minutes * team_poss) as numeric),1),
            tov100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                tov * team_minutes / (minutes * team_poss) desc)),
            astpct:=round(cast(100.0 * ast * team_minutes / (minutes*(team_fgm-fgm)) as numeric),1),
            astpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ast * team_minutes / (minutes*(team_fgm-fgm)) asc)),
            tovpct:=round(cast(100.0 * tov / (fga + (0.44 * fta) + tov) as numeric),1),
            tovpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                tov / (fga + (0.44 * fta) + tov) desc)),
            atr:=ast/nullif(tov, 0),
            atrpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                ast/nullif(tov, 0) asc))
        ) as passing,
        struct_pack(
            orb100:=round(cast(100.0 * orb * team_minutes / (minutes * team_poss) as numeric),1),
            drb100:=round(cast(100.0 * drb * team_minutes / (minutes * team_poss) as numeric),1),
            orbpct:=round(cast(100.0 * orb * team_minutes / (minutes * (team_orb+opp_drb)) as numeric),1),
            drbpct:=round(cast(100.0 * drb * team_minutes / (minutes * (team_drb+opp_orb)) as numeric),1),
            orb100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                orb * team_minutes / (minutes * team_poss))),
            drb100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                drb * team_minutes / (minutes * team_poss))),
            orbpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                orb * team_minutes / (minutes * (team_orb+opp_drb)))),
            drbpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                drb * team_minutes / (minutes * (team_drb+opp_orb))))
        ) as rebounding,
        struct_pack(
            stl100:=round(cast(100.0 * stl * team_minutes / (minutes * team_poss) as numeric),1),
            stl100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                stl * team_minutes / (minutes * team_poss))),
            blk100:=round(cast(100.0 * blk * team_minutes / (minutes * team_poss) as numeric),1),
            blk100pctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                blk * team_minutes / (minutes * team_poss))),
            blkpct:=round(cast(100.0 * blk * team_minutes / (minutes*(opp_fga-opp_fg3a)) as numeric),1),
            blkpctpctile:=100.0*(percent_rank() over (partition by experience_abbreviation order by
                blk * team_minutes / (minutes*(opp_fga-opp_fg3a))))
        ) as defense
    from stats s join {'stage_players_women' if women else 'stage_players'} p on s.player_id=p.id
    left join {'stage_rsci_rankings' if not women else 'stage_hoopgurlz_rankings'} rsci on p.full_name=rsci.{'Player' if not women else 'name'}
    left join stage_prospect_birthdays b on p.full_name=b.name
    left join (select name, agency from read_csv('data/raw/2025/early_entrants/data.csv')) ee on p.full_name=ee.name
    -- left join stage_combine_measurements combine on p.full_name=combine.player_name
    join {'stage_teams_women' if women else 'stage_teams'} t on p.team_id=t.id
    join {'stage_team_ratings_women' if women else 'stage_team_ratings'} tr on p.team_id=tr.team_id
    join sos on p.team_id=sos.team_id
    order by ez_poss_pctile desc;
    """