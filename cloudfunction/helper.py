import logging
import os
import time
from datetime import datetime, timedelta
from random import randint

import config
import polars as pl
import requests
from google.cloud import storage

SCHEMA_APARTMENT = {
    "url": pl.Utf8,
    "area": pl.Float64,
    "yearBuilt": pl.Utf8,
    "numberOfRooms": pl.Int64,
    "category": pl.Utf8,
    "postedTime": pl.Utf8,
    "renewalTime": pl.Utf8,
    "price": pl.Float64,
    "priceM2": pl.Float64,
    "previousPrice": pl.Float64,
    "priceReductionPercentage": pl.Float64,
    "title": pl.Utf8,
    "code": pl.Int64,
    "priceCurrency": pl.Utf8,
    "isPromoted": pl.Boolean,
    "country": pl.Utf8,
    "region": pl.Utf8,
    "city": pl.Utf8,
    "neighborhood": pl.Utf8,
    "extractDate": pl.Utf8,
}

SCHEMA_CARS = {
    "url": pl.Utf8,
    "makeYear": pl.Utf8,
    "mileage": pl.Float64,
    "power": pl.Float64,
    "category": pl.Utf8,
    "postedTime": pl.Utf8,
    "renewalTime": pl.Utf8,
    "price": pl.Float64,
    "previousPrice": pl.Float64,
    "priceReductionPercentage": pl.Float64,
    "title": pl.Utf8,
    "code": pl.Int64,
    "priceCurrency": pl.Utf8,
    "isPromoted": pl.Boolean,
    "country": pl.Utf8,
    "region": pl.Utf8,
    "city": pl.Utf8,
    "neighborhood": pl.Utf8,
    "extractDate": pl.Utf8,
}


class RequestsHelper:
    def __init__(self, category: str):
        self.items_per_page = 50
        self.cookies = config.COOKIES
        self.headers = config.HEADERS
        self.bucket_name = os.environ["BUCKET_NAME"]
        self.day_of_week = datetime.now().strftime("%A").lower()
        self.week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        self.full_refresh_flag = self._set_full_refresh_flag()
        if category == "flats-for-sale":
            self._base_url = (
                "https://www.index.hr/oglasi/nekretnine/prodaja-stanova/oglas/"
            )
            self._schema = SCHEMA_APARTMENT
            self.additional_params = {
                "module": "real-estate",
                "includeCityIds": "36bd1dbc-f301-4ae7-988e-171765d599f7"
            }
        elif category == "car":
            self._base_url = (
                "https://www.index.hr/oglasi/auto-moto/osobni-automobili/oglas/"
            )
            self._schema = SCHEMA_CARS
            self.additional_params = {
                "module": "vehicles",
                "includeCountyIds": "95c57539-6835-4b89-8ddd-a5de2100686e",
                "priceFrom": "2000",
                "priceTo": "6000",
            }

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
        df = df.with_columns(
            pl.concat_str(
                [
                    pl.lit(self._base_url),
                    pl.col("smartLink"),
                    pl.lit("/"),
                    pl.col("code"),
                ]
            ).alias("url")
        )
        return df

    def _set_full_refresh_flag(self) -> bool:
        return self.day_of_week in ["friday"]

    def get_json_response(self, url: str, params: dict = {}) -> dict:
        response = requests.get(
            url, params=params, cookies=self.cookies, headers=self.headers
        )
        return response.json()

    def enforce_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.cast(self._schema)

    def modify_df(self, df: pl.DataFrame) -> pl.DataFrame:
        df = self._extract_location(df)
        df = self._build_url_column(df)
        df = df.unnest("summary")

        # drop columns not in schema
        columns_to_drop = [col for col in df.columns if col not in self._schema.keys()]
        df = df.drop(columns_to_drop, strict=False)

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
