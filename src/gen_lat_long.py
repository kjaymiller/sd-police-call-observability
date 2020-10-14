from datetime import time
from pathlib import Path
from elasticsearch import Elasticsearch
import typing
import eland
import pandas as pd
import geopandas as gpd


es = Elasticsearch()
geodf = eland.DataFrame(es, "sd_geo")


def get_location_points(gdf, point, threshold=100):
    q = {
        "query": {
            "bool": {
                "must": [
                    {"match_all": {}},
                ],
                "filter": {
                    "range": {
                        "number": {
                            "gte": point.address_number_primary - threshold,
                            "lte": point.address_number_primary + threshold,
                        },
                    },
                },
            },
        },
    }

    results = gdf.es_query(q)
    lat = results["latitude"].median()
    lon = results["longitude"].median()

    return [lat, lon]


def get_location(df, point: str) -> typing.Tuple[int]:
    """Check the lat/long of a document and add return the median object"""

    q = {
        "query": {
            "wildcard": {
                "street.keyword": f"{point}*",
            },
        },
    }
    gdf = geodf.es_query(q)
    new_df = df.copy()
    new_df["location"] = df.apply(lambda x: get_location_points(gdf, x), axis=1)
    return new_df


def build_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    df["month_of_year"] = pd.to_datetime(df["date_time"]).dt.month
    df["day_of_month"] = pd.to_datetime(df["date_time"]).dt.day
    return df


@click.command()
@click.argument("--filepath", help="CSV File to be processed and indexed")
def build_index(filepath):
    """Load the corresponding csv file into a pandas dataframe and query each
    point against our sd_geo index appending the median lat/longs returned
    """
    df = pd.read_csv(filepath).fillna("")
    df = build_datetimes(df)

    # filters out only points with a valid address
    address_points = df.loc[(df.address_number_primary != 0)]
    points = address_points.address_road_primary.unique()

    for point in points:
        click.echo(f"fecthing {point}")
        road = df.loc[(df.address_road_primary == point)]
        new_df = get_location(road, point)
        eland.pandas_to_eland(
            pd_df=new_df,
            es_client=es,
            es_dropna=True,
            es_dest_index="pd_calls_for_service_2015",
            es_if_exists="append",  # so that these items are overwriting existing records. This allows us to update existing indexes.
            es_type_overrides={"date_time": "date"},
        )


if __name__ == "__main__":
    gen_csv("./assets/pd_calls_for_service_2015_datasd.csv")

    # for _,row in df.iterrows():
    #    print(get_location(row.address_road_primary, row.address_number_primary))
    #    break
