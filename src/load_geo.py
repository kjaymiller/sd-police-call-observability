import json
import pandas as pd
import geopandas as geopd
import click
from elasticsearch_dsl import (
        Document,
        GeoPoint,
        Integer,
        Text,
        Index,
        Float, 
        )
from elasticsearch.helpers import parallel_bulk
from elasticsearch import Elasticsearch


es = Elasticsearch()


@click.group()
def cli():
    pass


def convert_data(json_data):
    """Transforms the geopandas dataframe into a json file to be easily loaded
    into elasticsearch. Eland would be better suited for this but currently
    does not support geopandas `Point` data.
    """

    for row in json.loads(json_data)["features"]:
        point = row["properties"]
        point["coordinates"] = row['geometry']['coordinates'] 
        yield point


index = Index("sd_geo")


@index.document
class SDGeo(Document):
    """Document structure for the sd_geo index"""

    location = GeoPoint()
    number = Integer
    street = Text()
    latitude = Float()
    longitude = Float()


def load_geo_json_data(json_data):
    """Generator to create a new sd_geo document"""

    for i in convert_data(json_data):
        sd_geo_coordinate = SDGeo(
            location=i["coordinates"],
            number=int(i["number"]),
            street=i["street"],
            latitude=float(i["coordinates"][0]),
            longitude=float(i["coordinates"][1]),
        )

        yield sd_geo_coordinate.to_dict(include_meta=True)


def load_file(address_file):
    gdf = geopd.read_file(address_file)
    return gdf.to_json()


@cli.command()
@click.option(
    "-F", "--force", is_flag=True, help="Add the items, deleting an existing index"
)
@click.argument("geo_file", type=click.Path(exists=True))
def index_geo(force, geo_file):
    """loads geoJSON file to your elasticsearch index"""

    if index.exists(es):

        if force:
            index.delete(es)

        else:
            raise AttributeError(
                f"{index=} already exists. \
                Delete the index and try again or use -F to force the update"
            )

    gdf_json = load_file(geo_file)

    for _ in parallel_bulk(es, index="sd_geo", actions=load_geo_json_data(gdf_json)):
        pass


if __name__ == "__main__":
    cli()
