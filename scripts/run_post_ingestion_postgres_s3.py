import argparse
import os
import minio
import dotenv

import sqlalchemy as sa
import sqlalchemy.engine.url as url
from loguru import logger
from selenium import webdriver

from library.io_interfaces.filestore_io import S3FSInterface
from library.io_interfaces.db_io import PostgresInterface
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
    "-b", "--bucket_name", help="The bucket in S3 storage where data will be written"
)

parser.add_argument(
    "-db", "--db_uri", help="The URI that is used to connect to the postgres database"
)
args = parser.parse_args()

reddit_url: str = args.reddit_url

logger.info(f"Loading secrets from env file {args.env_file}")
dotenv.load_dotenv(args.env_file)

if __name__ == "__main__":

    MINIO_CLIENT = minio.Minio(
        os.environ.get("MINIO_URL"),
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
        secure=False,
    )

    DB_URI = url.make_url(args.db_uri)
    POSTGRES_ENGINE: sa.engine.Engine = sa.create_engine(DB_URI)

    with POSTGRES_ENGINE.connect() as conn, conn.begin():

        table_create_query = sa.text(
            """

        CREATE SCHEMA IF NOT EXISTS core;

        CREATE TABLE IF NOT EXISTS core.source (
            id UUID PRIMARY KEY,
            type TEXT NOT NULL,
            created_date TIMESTAMPTZ NOT NULL,
            fields JSONB NOT NULL
        );
        """
        )

        conn.execute(table_create_query)

    sqlite_localfiles_config = {
        "reddit_username": os.environ.get("REDDIT_USERNAME"),
        "reddit_password": os.environ.get("REDDIT_PASSWORD"),
        "db_engine": POSTGRES_ENGINE,
        "MINIO_CLIENT": MINIO_CLIENT,
        "root_dir_name": args.bucket_name,
        "content_type": "",
    }

    driver = webdriver.Chrome()
    driver.implicitly_wait(30)

    inserted_reddit_post_ids: list[str] = []

    logger.info(f"Ingesting Reddit Posts into db:")
    # postgresql+psycopg2://myuser:mypassword@localhost:5432/mydatabase
    recursive_insert_raw_reddit_post(
        driver=driver,
        page_url=reddit_url,
        config=sqlite_localfiles_config,
        inserted_reddit_ids=inserted_reddit_post_ids,
        file_io=S3FSInterface,
        database_io=PostgresInterface,
        login=True,
    )
