import logging
import os
import time
from datetime import datetime, timedelta
from random import randint

import config
import polars as pl
import requests
from google.cloud import storage


class RequestsHelper:
    def __init__(self):
        self.items_per_page = 50
        self.cookies = config.COOKIES
        self.headers = config.HEADERS
        self.columns_to_drop = config.COLUMNS_TO_DROP
        self.bucket_name = os.environ["BUCKET_NAME"]
        self.day_of_week = datetime.now().strftime("%A").lower()
        self.week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        self.full_refresh_flag = self._set_full_refresh_flag()

    def _extract_location(self, df: pl.DataFrame) -> pl.DataFrame:
        columns = [
            (0, "country"),
            (1, "region"),
            (2, "city"),
            (3, "neighborhood"),
        ]

        for index, column in columns:
            df = df.with_columns(
                pl.col("permutiveData")
                .struct.field("location")
                .list.get(index, null_on_oob=True)
                .alias(column)
            )

        return df

    def _build_url_column(self, df: pl.DataFrame) -> pl.DataFrame:
        # concat hardcoded, smartlink and code
        base_url = "https://www.index.hr/oglasi/nekretnine/prodaja-stanova/oglas/"

        df = df.with_columns(
            pl.concat_str(
                [pl.lit(base_url), pl.col("smartLink"), pl.lit("/"), pl.col("code")]
            ).alias("url")
        )
        return df

    def _set_full_refresh_flag(self) -> bool:
        return self.day_of_week in ["sunday"]

    def get_json_response(self, url: str, params: dict = {}) -> dict:
        response = requests.get(
            url, params=params, cookies=self.cookies, headers=self.headers
        )
        return response.json()

    def enforce_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        schema = config.SCHEMA
        return df.cast(schema)

    def modify_df(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._extract_location(df)
        df = self._build_url_column(df)
        df = df.unnest("summary")
        df = df.drop(self.columns_to_drop)

        # add extract date
        df = df.with_columns(
            pl.lit(datetime.now().strftime("%Y-%m-%d")).alias("extractDate")
        )
        return df

    def random_sleep(self):
        sleep_time = randint(1, 2)
        time.sleep(sleep_time)

    def upload_to_gcs(self, blob_name: str):
        client = storage.Client()
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(blob_name)
        logging.info(f"Uploaded {blob_name} to gs://{self.bucket_name}/{blob_name}")

    def check_if_done_fetching(self, latest_date) -> bool:
        return latest_date <= self.week_ago
