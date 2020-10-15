import click
from datetime import time
from pathlib import Path
from elasticsearch import Elasticsearch
import typing
import eland
import pandas as pd
import geopandas as gpd


es = Elasticsearch()
geodf = eland.DataFrame(es, "sd_geo")


def get_closest_lat_long(point, threshold=100):
    q = {
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "street.keyword": f"{point.address_road_primary}*",
                            },
                        },
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
        "sort": {
            "_script": {
                "type": "number",
                "script": {
                    "lang": "painless",
                    "source": f"Math.abs(doc['number'].value-{point.address_number_primary})",
                },
                "order": "asc",
            }
        },
    }
    result = geodf.es_query(q)
    return [result.latitude, result.longitude]


def build_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    df["month_of_year"] = pd.to_datetime(df["date_time"]).dt.month
    df["day_of_month"] = pd.to_datetime(df["date_time"]).dt.day
    return df


@click.command()
@click.argument(
    "filepath",
    type=click.Path(exists=True),
)
def build_index(filepath):
    """Load the corresponding csv file into a pandas dataframe and query each
    point against our sd_geo index appending the median lat/longs returned
    """
    df = pd.read_csv(filepath).fillna("")
    df = build_datetimes(df)

    # filters out only points with a valid address
    address_points = df.loc[(df.address_number_primary != 0)]
    address_points["location"] = address_points.apply(lambda x: get_closest_lat_long(x), axis=1)
    eland.pandas_to_eland(
        pd_df=address_points,
        es_client=es,
        es_dropna=True,
        es_dest_index=Path(filepath).stem,
        es_if_exists="replace",  # so that these items are overwriting existing records. This allows us to update existing indexes.
        es_type_overrides={"date_time": "date"},
        )


if __name__ == "__main__":
    build_index()

    # for _,row in df.iterrows():
    #    print(get_location(row.address_road_primary, row.address_number_primary))
    #    break
