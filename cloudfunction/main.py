import logging
import os
from datetime import datetime

import functions_framework
import polars as pl
from flask import Request, jsonify
from google.cloud.logging import Client
from helper import RequestsHelper

client = Client()
client.setup_logging()


@functions_framework.http
def main(request: Request):
    category = os.environ["CATEGORY"]
    current_date = datetime.now().strftime("%Y-%m-%d")

    requests_helper = RequestsHelper()

    logging.info(
        f"Full refresh (wednesday, sunday): {requests_helper.full_refresh_flag}. DOW: {requests_helper.day_of_week}"
    )
    params = {
        "category": category,
        "module": "real-estate",
        "includeCityIds": "36bd1dbc-f301-4ae7-988e-171765d599f7",
        "page": "1",
        "sortOption": "4",
        "itemPerPage": requests_helper.items_per_page,
    }
    url = "https://www.index.hr/oglasi/api/aditem"

    json_data = requests_helper.get_json_response(
        url,
        params,
    )
    max_count = json_data["count"]
    pages = max_count // requests_helper.items_per_page + 1

    logging.info(f"Fetching data for category: {category}")
    logging.info(f"Max count: {max_count}, Pages: {pages}")
    for page in range(1, pages + 1):
        params["page"] = str(page)
        json_data = requests_helper.get_json_response(
            url,
            params,
        )
        df = pl.DataFrame(json_data["data"])
        if df.is_empty():
            continue

        df = requests_helper.modify_df(df)

        # enforce schema
        df = requests_helper.enforce_schema(df)

        # concat dfs
        if page == 1:
            df_all = df
        else:
            df_all = df_all.vstack(df)

        requests_helper.random_sleep()
        logging.info(f"Page {page}/{pages} done")

        # early stop if not full refresh and done fetching last 7 days
        if not requests_helper.full_refresh_flag:
            latest_date = df.select(pl.col("renewalTime").max()).to_series()[0]
            # skip if latest_date is None
            if not latest_date:
                continue
            if requests_helper.check_if_done_fetching(latest_date):
                logging.info(
                    f"Done fetching recent 7 days data, stopping early. Latest date: {latest_date}"
                )
                break

    # create folder
    data_folder = f"data/{category}/{current_date}"
    os.makedirs(data_folder, exist_ok=True)

    blob_name = f"{data_folder}/data_{current_date}.parquet"
    df_all.write_parquet(blob_name, compression="zstd")
    requests_helper.upload_to_gcs(blob_name)

    result = {"message": "Success"}
    return jsonify(result), 200
