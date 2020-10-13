# SD Police Call Observability

## LICENSE

This project is available to use under the [MIT LICENSE](./LICENSE).

## Requirements

- Python Environment: 3.8+
- Python packages used available in [requirements.in](./requirements.in)
- Elasticsearch/Kibana 7.91+

## Contributing
Check out the [Contribution Guidelines](./CONTRIBUTING.md)

## Download the Datasets
This repo does not contain any of the data (as it's kinda large)

- Use [download_datasets.py](./src/download_datasets.py) to download one or more years of data.

## Getting the address geoJSON Data

The geoJSON data that we will use is from <https://openaddresses.io>. You will need to make an account to download this data. 

**NOTE**: This dataset can update. I would recommend that if your data seems to be getting less accurate, I would suggest that you update your geoJSON file.

Once you have downloaded the geoJSON file, run [load_geo.py](./src/load_geo.py) and supply the path to the geoJSON file as an argument.
