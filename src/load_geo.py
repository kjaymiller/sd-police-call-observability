import json
import pandas as pd
import geopandas as geopd
import click
from elasticsearch_dsl import Document, GeoPoint, Integer, Text
from elasticsearch.helpers import parallel_bulk
from elasticsearch import Elasticsearch


es = Elasticsearch()


@click.group()
def cli():
    pass


def convert_data(json_data):
    for row in json.loads(json_data)["features"]:
        point = row["properties"]
        point["coordinates"] = row["geometry"]["coordinates"]
        yield point


index = Index("sd_geo")


@index.document
class SDGeo(Document):
    coordinates = (GeoPoint(),)
    number = (Text(),)
    street = (Text(),)


def load_geo_json_data(json_data):
    for i in convert_data(json_data):
        sd_geo_coordinate = SDGeo(
            coordinates=i["coordinates"],
            number=i["number"],
            street=i["street"],
        )
        yield sd_geo_coordinate.to_dict(include_meta=True)


def load_file(address_file):
    gdf = geopd.read_file(address_file)
    return gdf.to_json()


@cli.command()
@click.option(
    "-F", "--force", default=False, help="Add the items, deleting an existing index"
)
def index_geo(force):

    if index.exists:

        if force:
            index.delete()

        else:
            raise AttributeError(
                f"{index=} already exists. \
                Delete the index and try again or use -F to force the update"
            )

    gdf_json = load_file("./us_ca_city_of_san_diego-addresses-city.geojson")

    for _ in parallel_bulk(es, index="sd_geo", actions=load_geo_json_data(gdf_json)):
        pass


if __name__ == "__main__":
    cli()
