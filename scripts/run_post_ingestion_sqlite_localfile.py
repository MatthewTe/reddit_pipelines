import argparse
import os
import minio
import dotenv
import sys

import pysqlite3

sys.modules["sqlite3"] = pysqlite3

import sqlalchemy as sa
from geoalchemy2 import load_spatialite
from sqlalchemy.event import listen

import sqlalchemy.engine.url as url
from loguru import logger
from selenium import webdriver

from library.io_interfaces.filestore_io import LocalFSInterface
from library.io_interfaces.db_io import SQLiteInterface
from library.reddit_post_extraction_methods import recursive_insert_raw_reddit_post

parser = argparse.ArgumentParser()
parser.add_argument(
    "--env_file",
    "-e",
    help="The path to the environment file used to load all of the secrets",
)
parser.add_argument(
    "-u",
    "--reddit_url",
    help="The reddit url that is used to run the ingestion pipeline.",
)

parser.add_argument(
    "-f",
    "--file_directory",
    help="The full path for the root path where the static files will be stored",
)

parser.add_argument(
    "-db", "--sqlite_db_path", help="The full filepath to the SQlite database"
)
args = parser.parse_args()

reddit_url: str = args.reddit_url

logger.info(f"Loading secrets from env file {args.env_file}")
dotenv.load_dotenv(args.env_file)

if __name__ == "__main__":

    DB_URI = url.make_url(f"sqlite:////{args.sqlite_db_path}")
    SQLITE_ENGINE: sa.engine.Engine = sa.create_engine(DB_URI)
    listen(SQLITE_ENGINE, "connect", load_spatialite)

    with SQLITE_ENGINE.connect() as conn, conn.begin():

        source_table_create_query = sa.text(
            """
            CREATE TABLE IF NOT EXISTS source (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                created_date TIMESTAMP NOT NULL,
                fields TEXT
            );
            """
        )

        labels_table_create_query = sa.text(
            """
            CREATE TABLE IF NOT EXISTS labels (
                label_id TEXT PRIMARY KEY,
                post_id TEXT NOT NULL,
                comment TEXT
            );
            """
        )

        create_geometry_col_query = sa.text(
            """
            SELECT AddGeometryColumn('labels', 'geometry', 4326, 'POLYGON', 'XY');
            """
        )

        conn.execute(source_table_create_query)
        conn.execute(labels_table_create_query)
        conn.execute(create_geometry_col_query)

    sqlite_localfiles_config = {
        "reddit_username": os.environ.get("REDDIT_USERNAME"),
        "reddit_password": os.environ.get("REDDIT_PASSWORD"),
        "db_engine": SQLITE_ENGINE,
        "root_dir_name": args.file_directory,
        "content_type": "",
    }

    driver = webdriver.Chrome()
    driver.implicitly_wait(30)

    inserted_reddit_post_ids: list[str] = []

    logger.info(f"Ingesting Reddit Posts into db:")

    recursive_insert_raw_reddit_post(
        driver=driver,
        page_url=reddit_url,
        config=sqlite_localfiles_config,
        inserted_reddit_ids=inserted_reddit_post_ids,
        file_io=LocalFSInterface,
        database_io=SQLiteInterface,
        login=True,
    )
