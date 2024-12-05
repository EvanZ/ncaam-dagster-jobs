from typing import Literal
import warnings

import dagster
from dagster import (
    asset, 
    AssetExecutionContext, 
    MaterializeResult, 
    MetadataValue, 
    AssetsDefinition,
)
from dagster_duckdb import DuckDBResource
import fasteners
import numpy
import pandas
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import RidgeCV
from sklearn.metrics import root_mean_squared_error

from . import queries
from .constants import (
    SKLEARN,
    MODELS,
    MODELS_WOMEN
    )

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

def build_stage_box_stat_adjustment_factors(stat: Literal["tov", "fta", "ftm", "fga", "fg3a", "fg3m", 
                                                          "orb", "ast", "stl", "blk", "drb"],
                                       higher_is_better: bool=True,
                                       offense: bool=True,
                                       women: bool=False,
                                       alphas: list[float]=[0.1, 1.0, 10.0]) -> AssetsDefinition:
    """
    factory function for building models for a stat in stage_game_logs
    """
    @asset(
        deps=["stage_game_logs_women" if women else "stage_game_logs"],
        name=f"adj_{stat}_per_100_model{'_women' if women else ''}",
        group_name=MODELS_WOMEN if women else MODELS,
        compute_kind=SKLEARN,
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
                    from {'stage_game_logs_women' if women else 'stage_game_logs'}
                    where team_1_pts is not null
                    union all
                    select
                        game_id,
                        poss,
                        1.0 as home,
                        'o' || team_2_id as offense,
                        'd' || team_1_id as defense,
                        100.0*team_{team_1_or_2(not offense)}_stats.{stat}/poss as rating
                    from {'stage_game_logs_women' if women else 'stage_game_logs'}
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
        rmse = root_mean_squared_error(y_test, y_pred)
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
                conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {'stage_box_stat_adjustment_factors_women' if women else 'stage_box_stat_adjustment_factors'} (
                    team_id INT,
                    stat VARCHAR,
                    ortg DOUBLE,
                    drtg DOUBLE,
                    PRIMARY KEY (team_id, stat)
                    );
                """)

                query = f"""
                    INSERT OR IGNORE INTO {'stage_box_stat_adjustment_factors_women' if women else 'stage_box_stat_adjustment_factors'} 
                    (
                    SELECT 
                        st.id as team_id, 
                        '{stat}' as stat,
                        o.rating / avg(o.rating) over () as ortg,
                        d.rating / avg(d.rating) over () as drtg
                    FROM ortg_df o
                    JOIN {'stage_teams_women' if women else 'stage_teams'} st ON o.id=st.id
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

tov_model_women = build_stage_box_stat_adjustment_factors(stat="tov", higher_is_better=False, women=True, alphas=alphas)
ast_model_women = build_stage_box_stat_adjustment_factors(stat="ast", women=True, alphas=alphas)
fta_model_women = build_stage_box_stat_adjustment_factors(stat="fta", women=True, alphas=alphas)
ftm_model_women = build_stage_box_stat_adjustment_factors(stat="ftm", women=True, alphas=alphas)
fga_model_women = build_stage_box_stat_adjustment_factors(stat="fga", women=True, alphas=alphas)
fg3a_model_women = build_stage_box_stat_adjustment_factors(stat="fg3a", women=True, alphas=alphas)
fg3m_model_women = build_stage_box_stat_adjustment_factors(stat="fg3m", women=True, alphas=alphas)
orb_model_women = build_stage_box_stat_adjustment_factors(stat="orb", women=True, alphas=alphas)
drb_model_women = build_stage_box_stat_adjustment_factors(stat="drb", offense=False, women=True, alphas=alphas)
stl_model_women = build_stage_box_stat_adjustment_factors(stat="stl", offense=False, women=True, alphas=alphas)
blk_model_women = build_stage_box_stat_adjustment_factors(stat="blk", offense=False, women=True, alphas=alphas)

def build_stage_shot_type_adjustment_factors(shot: Literal["ast_dunk", "unast_dunk", "miss_dunk", 
                                                           "ast_layup", "unast_layup", "miss_layup", 
                                                           "ast_mid", "unast_mid", "miss_mid",
                                                           "ast_3pt", "unast_3pt", "miss_3pt",
                                                           "ast_tip", "unast_tip", "miss_tip"],
                                       higher_is_better: bool=True,
                                       women: bool=False,
                                       alphas: list[float]=[0.1, 1.0, 10.0]) -> AssetsDefinition:
    """
    factory function for building models for a shot type in stage_player_shots_by_game
    """
    @asset(
        deps=[f"{'stage_game_logs_women' if women else 'stage_game_logs'}", 
              f"{'stage_player_shots_by_game_women' if women else 'stage_player_shots_by_game'}"],
        name=f"adj_{shot}_per_100_model{'_women' if women else ''}",
        group_name=MODELS_WOMEN if women else MODELS,
        compute_kind=SKLEARN,
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
                        from {'stage_player_shots_by_game_women' if women else 'stage_player_shots_by_game'}
                        group by ALL
                    ),
                    games as (
                        select
                            game_id as gid,
                            poss
                        from {'stage_game_logs_women' if women else 'stage_game_logs'}
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
        rmse = root_mean_squared_error(y_test, y_pred)
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
                conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {'stage_shot_type_adjustment_factors_women' if women else 'stage_shot_type_adjustment_factors'} (
                    team_id INT,
                    stat VARCHAR,
                    ortg DOUBLE,
                    drtg DOUBLE,
                    PRIMARY KEY (team_id, stat)
                    );
                """)

                query = f"""
                    INSERT OR IGNORE INTO {'stage_shot_type_adjustment_factors_women' if women else 'stage_shot_type_adjustment_factors'} 
                    (
                    SELECT 
                        st.id as team_id, 
                        '{shot}' as stat,
                        o.rating / avg(o.rating) over () as ortg,
                        d.rating / avg(d.rating) over () as drtg
                    FROM ortg_df o
                    JOIN {'stage_teams_women' if women else 'stage_teams'} st ON o.id=st.id
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

ast_3pt_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="ast_3pt", alphas=alphas)
unast_3pt_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="unast_3pt", alphas=alphas)
miss_3pt_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="miss_3pt", higher_is_better=False, alphas=alphas)
ast_dunk_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="ast_dunk", alphas=alphas)
unast_dunk_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="unast_dunk", alphas=alphas)
miss_dunk_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="miss_dunk", higher_is_better=False, alphas=alphas)
ast_layup_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="ast_layup", alphas=alphas)
unast_layup_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="unast_layup", alphas=alphas)
miss_layup_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="miss_layup", higher_is_better=False, alphas=alphas)
ast_mid_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="ast_mid", alphas=alphas)
unast_mid_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="unast_mid", alphas=alphas)
miss_mid_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="miss_mid", higher_is_better=False, alphas=alphas)
ast_tip_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="ast_tip", alphas=alphas)
unast_tip_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="unast_tip", alphas=alphas)
miss_tip_shot_type_adjustment = build_stage_shot_type_adjustment_factors(shot="miss_tip", higher_is_better=False, alphas=alphas)

ast_3pt_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="ast_3pt", women=True, alphas=alphas)
unast_3pt_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="unast_3pt", women=True, alphas=alphas)
miss_3pt_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="miss_3pt", women=True, higher_is_better=False, alphas=alphas)
ast_dunk_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="ast_dunk", women=True, alphas=alphas)
unast_dunk_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="unast_dunk", women=True, alphas=alphas)
miss_dunk_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="miss_dunk", women=True, higher_is_better=False, alphas=alphas)
ast_layup_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="ast_layup", women=True, alphas=alphas)
unast_layup_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="unast_layup", women=True, alphas=alphas)
miss_layup_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="miss_layup", women=True, higher_is_better=False, alphas=alphas)
ast_mid_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="ast_mid", women=True, alphas=alphas)
unast_mid_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="unast_mid", women=True, alphas=alphas)
miss_mid_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="miss_mid", women=True, higher_is_better=False, alphas=alphas)
ast_tip_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="ast_tip", women=True, alphas=alphas)
unast_tip_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="unast_tip", women=True, alphas=alphas)
miss_tip_shot_type_adjustment_women = build_stage_shot_type_adjustment_factors(shot="miss_tip", women=True, higher_is_better=False, alphas=alphas)

def build_stage_team_ratings(higher_is_better: bool=True,
                            women: bool=False,
                            alphas: list[float]=[0.1, 1.0, 10.0]) -> AssetsDefinition:
    """
    factory function for building models for a stat in stage_game_logs
    """
    @asset(
        deps=["stage_game_logs_women" if women else "stage_game_logs"],
        name=f"adj_team_ratings{'_women' if women else ''}",
        group_name=MODELS_WOMEN if women else MODELS,
        compute_kind=SKLEARN,
        description=f"model for predicting team ratings"
    )
    def _asset(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
        """
        use catboost model to do regression on game results
        """

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
                        100.0*team_1_pts/poss as rating
                    from {'stage_game_logs_women' if women else 'stage_game_logs'}
                    where (team_1_pts is not null) and not ((team_1_logo is null) or (team_2_logo is null))
                    union all
                    select
                        game_id,
                        poss,
                        1.0 as home,
                        'o' || team_2_id as offense,
                        'd' || team_1_id as defense,
                        100.0*team_2_pts/poss as rating
                    from {'stage_game_logs_women' if women else 'stage_game_logs'}
                    where (team_2_pts is not null) and not ((team_1_logo is null) or (team_2_logo is null)))
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
        rmse = root_mean_squared_error(y_test, y_pred)
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
                conn.execute(f"""
                DROP TABLE IF EXISTS {'stage_team_ratings_women' if women else 'stage_team_ratings'};
                """)
                conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {'stage_team_ratings_women' if women else 'stage_team_ratings'} (
                    team_id INT,
                    ortg DOUBLE,
                    drtg DOUBLE,
                    orank INT,
                    drank INT,
                    rank INT,
                    PRIMARY KEY (team_id)
                    );
                """)

                query = f"""
                    INSERT OR IGNORE INTO {'stage_team_ratings_women' if women else 'stage_team_ratings'} 
                    (
                    SELECT 
                        st.id as team_id, 
                        o.rating as ortg,
                        d.rating as drtg,
                        rank() over (order by o.rating desc) as orank,
                        rank() over (order by d.rating asc) as drank,
                        rank() over (order by (o.rating-d.rating) desc) as rank
                    FROM ortg_df o
                    JOIN {'stage_teams_women' if women else 'stage_teams'} st ON o.id=st.id
                    JOIN drtg_df d on d.id=st.id
                    )
                    returning team_id, ortg, drtg, orank, drank, rank;
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

adj_team_ratings_model = build_stage_team_ratings(alphas=alphas)
adj_team_ratings_model_women = build_stage_team_ratings(alphas=alphas, women=True)