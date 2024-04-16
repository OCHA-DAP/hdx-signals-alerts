#!/usr/bin/python
"""
Generic Blob into HDX Pipeline:
------------

TODO
- Add summary about this dataset pipeline

"""
import logging
from datetime import datetime
import pandas as pd
from hdx.data.dataset import Dataset
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

    def get_data(self, state):
        account = self.configuration["account"]
        container = self.configuration["container"]
        # TODO: move to get this key separately
        key = self.configuration["key"]
        blob = self.configuration["blob"]
        url = self.configuration["url"]
        dataset_name = self.configuration["dataset_names"]["HDX-SIGNALS"]

        downloaded_file = self.retriever.download_file(
            url=url,
            account=account,
            container=container,
            key=key,
            blob=blob)

        data_df = pd.read_csv(downloaded_file, sep=",", escapechar='\\').replace('[“”]', '', regex=True)
        self.dataset_data[dataset_name] = data_df.apply(lambda x: x.to_dict(), axis=1)

        # TODO
        # add date logic

        if self.dataset_data:
            return [{"name": dataset_name}]
        else:
            return None

    def generate_dataset(self, dataset_name):

        # Setting metadata and configurations
        name = self.configuration["dataset_names"]["HDX-SIGNALS"]
        title = self.configuration["title"]
        update_frequency = self.configuration["update_frequency"]
        dataset = Dataset({"name": slugify(name), "title": title})
        rows = self.dataset_data[dataset_name]
        dataset.set_maintainer(self.configuration["maintainer_id"])
        dataset.set_organization(self.configuration["organization_id"])
        dataset.set_expected_update_frequency(update_frequency)
        dataset.set_subnational(False)
        dataset.add_other_location("world")
        dataset["notes"] = self.configuration["notes"]
        filename = f"{dataset_name.lower()}.csv"
        resource_data = {"name": filename,
                         "description": self.configuration["description"]}
        tags = sorted([t for t in self.configuration["allowed_tags"]])
        dataset.add_tags(tags)

        # Setting time period
        start_date = self.configuration["start_date"]
        ongoing = False
        if not start_date:
            logger.error(f"Start date missing for {dataset_name}")
            return None, None
        dataset.set_time_period(start_date, self.created_date, ongoing)

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

        return dataset
