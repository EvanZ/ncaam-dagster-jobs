import warnings

import dagster
from dagster import (
    asset, 
    AssetExecutionContext, 
    MaterializeResult, 
    MetadataValue 
)
from dagster_duckdb import DuckDBResource
import numpy
import pandas
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import RidgeCV
from sklearn.metrics import root_mean_squared_error

from .constants import (
    SKLEARN,
    )
from ..partitions import daily_partition
from ..resources import LocalFileStorage, JinjaTemplates
from ..utils.utils import fetch_data

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

@asset(
    deps=["stage_game_logs"],
    compute_kind=SKLEARN
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
    rmse = root_mean_squared_error(y_test, y_pred)
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
    deps=["stage_game_logs"],
    compute_kind=SKLEARN
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
    rmse = root_mean_squared_error(y_test, y_pred)
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