# SD Police Call Observability

The goal of this project is to use the Elastic Stack along with pandas,  geopandas, and the elasticsearch python toolset to index and query call data to make observations that would normally be hard to visualize with the data in CSV files. 

## LICENSE

This project is available to use under the [MIT LICENSE](./LICENSE).

## Requirements

- Python Environment: 3.8+
- Python packages used available in [requirements.in](./requirements.in)
- Elasticsearch/Kibana 7.91+

## Contributing

Check out the [Contribution Guidelines](./CONTRIBUTING.md)

## Installing Elasticsearch and Kibana
For simplicity and accessibility, this project is using a local instance of elasticsearch which can be freely downloaded at [elastic.co/downloads](https://www.elastic.co/downloads/).

Install and extract the binaries.

_On macOS_

```
tar -xzf elasticsearch-<VERSION_NUMBER>-darwin-x86_64.tar.gz 
tar -xzf kibana-<VERSION_NUMBER>-darwin-x86_64.tar.gz
```

You will need to ensure that both elasticsearch and kibana (**in that order**) are running. You can set them to run in the background

_On macOS_

```
elasticsearch-<VERSION_NUMBER>-darwin-x86_64/bin/elasticsearch &
kibana-<VERSION_NUMBER>-darwin-x86_64/bin/kibana &
```

## Downloading Data
This repo does not contain any of the data (as it's kinda large). You will need to download the data prior to observing.

## Download the Datasets

- Use [download_datasets.py](./src/download_datasets.py) to download one or more years of data.

## Getting the address geoJSON Data

The geoJSON data that we will use is from <https://openaddresses.io>. You will need to make an account to download this data. 

**NOTE**: This dataset can update. I would recommend that if your data seems to be getting less accurate, I would suggest that you update your geoJSON file.

Once you have downloaded the geoJSON file, run [load_geo.py](./src/load_geo.py) and supply the path to the geoJSON file as an argument. This will add the geoJSON file as an index into your Elasticsearch environment.

## Query location data for 
# TODO
