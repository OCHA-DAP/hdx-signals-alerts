#!/usr/bin/python
"""
Generic Blob into HDX Pipeline:
------------

TODO
- Add summary about this dataset pipeline

"""
import logging
import os
from datetime import datetime, timezone
import pandas as pd
from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from slugify import slugify


logger = logging.getLogger(__name__)


class HDXSignals:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.manual_url = None
        self.dataset_data = {}
        self.errors = errors
        self.created_date = None
        self.start_date = None
        self.latest_date = None

    def get_data(self):

        try:
            url = os.environ["BLOB_URL"]
            account = os.environ["STORAGE_ACCOUNT"]
            container = os.environ["CONTAINER"]
            key = os.environ["KEY"]
        except Exception:
            url = self.configuration["url"]
            account = self.configuration["account"]
            container = self.configuration["container"]
            key = self.configuration["key"]

        alerts_filename = self.configuration["alerts_filename"]
        locations_filename = self.configuration["locations_filename"]
        metadata_filename = self.configuration["metadata_filename"]
        dataset_name = self.configuration["dataset_names"]["HDX-SIGNALS"]

        alerts_file = self.retriever.download_file(
            url=url,
            account=account,
            container=container,
            key=key,
            blob=alerts_filename)

        data_df_alerts = pd.read_csv(alerts_file, sep=",", escapechar='\\', keep_default_na=False).replace('[“”]', '', regex=True)
        data_df_alerts['date'] = pd.to_datetime(data_df_alerts['date'])

        # Find the minimum and maximum dates
        self.start_date = data_df_alerts['date'].min()
        self.latest_date = data_df_alerts['date'].max()

        locations_file = self.retriever.download_file(
            url=url,
            account=account,
            container=container,
            key=key,
            blob=locations_filename)

        data_df_locations = pd.read_csv(locations_file, sep=",", escapechar='\\', keep_default_na=False).replace('[“”]', '', regex=True)

        metadata_file = self.retriever.download_file(
            url=url,
            account=account,
            container=container,
            key=key,
            blob=metadata_filename)

        metadata_dict = pd.read_table(metadata_file, sep=",")
        data_df_locations.rename(columns={'iso3': 'Alpha-3 code'}, inplace=True)
        lat_lon_file = pd.read_csv("metadata/countries_codes_and_coordinates.csv", sep=",")

        latitude = []
        longitude = []
        for iso3 in data_df_locations['Alpha-3 code']:
            if iso3 not in ["CHL", "VNM"]:
                try:
                    lat = lat_lon_file.loc[lat_lon_file['Alpha-3 code'] == iso3, 'Latitude (average)'].iloc[0]
                    lon = lat_lon_file.loc[lat_lon_file['Alpha-3 code'] == iso3, 'Longitude (average)'].iloc[0]
                except Exception:
                    lat = "NA"
                    lon = "NA"
            else:
                lat = data_df_locations.loc[data_df_locations['Alpha-3 code'] == iso3, 'lat'].iloc[0]
                lon = data_df_locations.loc[data_df_locations['Alpha-3 code'] == iso3, 'lon'].iloc[0]
            latitude.append(lat)
            longitude.append(lon)

        colnames = ['Alpha-3 code', 'acaps_inform_severity', 'acled_conflict', 'idmc_displacement_conflict',
                    'idmc_displacement_disaster', 'ipc_food_insecurity', 'jrc_agricultural_hotspots']
        data_df_locations_subset = data_df_locations[colnames]

        data_df_locations_subset['Latitude'] = latitude
        data_df_locations_subset['Longitude'] = longitude

        #  TODO FIND A WAY TO WRITE THIS TO GIT? OR REMOVE?
        # data_df_alerts.to_csv("metadata/signals.csv", sep=';', encoding='utf-8', index=False)
        # data_df_locations_subset.to_csv("metadata/location_metadata.csv", sep=';', encoding='utf-8', index=False)

        self.dataset_data[dataset_name] = [data_df_alerts.apply(lambda x: x.to_dict(), axis=1),
                                           data_df_locations.apply(lambda x: x.to_dict(), axis=1),
                                           metadata_dict.apply(lambda x: x.to_dict(), axis=1)]

        self.created_date = datetime.fromtimestamp((os.path.getctime(alerts_file)), tz=timezone.utc)
        return [{"name": dataset_name}]

    def generate_dataset_and_showcase(self, dataset_name):

        # Setting metadata and configurations
        name = self.configuration["dataset_names"]["HDX-SIGNALS"]
        title = self.configuration["title"]
        update_frequency = self.configuration["update_frequency"]
        dataset = Dataset({"name": slugify(name), "title": title})
        rows = self.dataset_data[dataset_name][0]
        dataset.set_maintainer(self.configuration["maintainer_id"])
        dataset.set_organization(self.configuration["organization_id"])
        dataset.set_expected_update_frequency(update_frequency)
        dataset.set_subnational(False)
        dataset.add_other_location("world")
        dataset["notes"] = self.configuration["notes"]
        filename = "hdx_signals.csv"
        resource_data = {"name": filename,
                         "description": self.configuration["description_alerts_file"]}
        tags = sorted([t for t in self.configuration["allowed_tags"]])
        dataset.add_tags(tags)

        # Setting time period
        start_date = self.start_date
        ongoing = False
        if not start_date:
            logger.error(f"Start date missing for {dataset_name}")
            return None, None
        dataset.set_time_period(start_date, self.latest_date, ongoing)

        headers = rows[0].keys()
        date_headers = [h for h in headers if "date" in h.lower() and type(rows[0][h]) == int]
        for row in rows:
            for date_header in date_headers:
                row_date = row[date_header]
                if not row_date:
                    continue
                if len(str(row_date)) > 9:
                    row_date = row_date / 1000
                row_date = datetime.utcfromtimestamp(row_date)
                row_date = row_date.strftime("%Y-%m-%d")
                row[date_header] = row_date

        rows
        dataset.generate_resource_from_rows(
            self.folder,
            filename,
            rows,
            resource_data,
            list(rows[0].keys()),
            encoding='utf-8'
        )

        second_filename = "hdx_signals_location_metadata.csv"
        resource_data = {"name": second_filename,
                         "description": self.configuration["description_locations_file"]}
        rows = self.dataset_data[dataset_name][1]
        headers = rows[0].keys()
        date_headers = [h for h in headers if "date" in h.lower() and type(rows[0][h]) == int]
        for row in rows:
            for date_header in date_headers:
                row_date = row[date_header]
                if not row_date:
                    continue
                if len(str(row_date)) > 9:
                    row_date = row_date / 1000
                row_date = datetime.utcfromtimestamp(row_date)
                row_date = row_date.strftime("%Y-%m-%d")
                row[date_header] = row_date

        rows
        dataset.generate_resource_from_rows(
            self.folder,
            second_filename,
            rows,
            resource_data,
            list(rows[0].keys()),
            encoding='utf-8'
        )

        resource_data = {"name": "hdx_signals_data_dictionary.csv",
                         "description": self.configuration["description_metadata_file"]}
        rows = self.dataset_data[dataset_name][2]
        headers = rows[0].keys()
        date_headers = [h for h in headers if "date" in h.lower() and type(rows[0][h]) == int]
        for row in rows:
            for date_header in date_headers:
                row_date = row[date_header]
                if not row_date:
                    continue
                if len(str(row_date)) > 9:
                    row_date = row_date / 1000
                row_date = datetime.utcfromtimestamp(row_date)
                row_date = row_date.strftime("%Y-%m-%d")
                row[date_header] = row_date

        rows
        dataset.generate_resource_from_rows(
            self.folder,
            "hdx_signals_data_dictionary.csv",
            rows,
            resource_data,
            list(rows[0].keys()),
            encoding='utf-8'
        )

        if not self.configuration["visualization_link"]:
            return dataset, None

        showcase = Showcase(
            {
                "name": f"{slugify(dataset_name)}-showcase",
                "title": f"{dataset['title']} Showcase",
                "notes": dataset["notes"],
                "url": self.configuration["visualization_link"],
                "image_url": "https://raw.githubusercontent.com/OCHA-DAP/hdx-signals-alerts/main/config/HDXSignalsLogo_V2.png",
            }
        )
        showcase.add_tags(tags)

        return dataset, showcase
