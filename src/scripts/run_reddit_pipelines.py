from selenium import webdriver

from minio import Minio
import argparse
from loguru import logger

from library.reddit_post_extraction_methods import recursive_insert_raw_reddit_post
from library.config import (
    get_secrets,
    Secrets,
)
from library.ingest_reddit_video import ingest_all_video_data

parser = argparse.ArgumentParser()
parser.add_argument(
    "-e",
    "--env_file",
    help="The path to the environment file used to load all of the secrets",
)
parser.add_argument(
    "-u",
    "--reddit_url",
    help="The reddit url that is used to run the ingestion pipeline.",
)

args = parser.parse_args()

reddit_url: str = args.reddit_url
secrets: Secrets = get_secrets(args.env_file)

if __name__ == "__main__":

    MINIO_CLIENT = Minio(
        secrets["minio_url"],
        access_key=secrets["minio_access_key"],
        secret_key=secrets["minio_secret_key"],
        secure=False,
    )
    BUCKET_NAME = "reddit-posts"

    found = MINIO_CLIENT.bucket_exists(BUCKET_NAME)
    if not found:
        MINIO_CLIENT.make_bucket(BUCKET_NAME)
        logger.info("Created bucket", BUCKET_NAME)
    else:
        logger.info("Bucket", BUCKET_NAME, "already exists")

    logger.info(f"Running ingestion pipeline for reddit url {reddit_url}")

    driver = webdriver.Chrome()
    driver.implicitly_wait(30)

    inserted_reddit_post_ids: list[str] = []

    logger.info(f"Ingesting Reddit Posts into db:")

    recursive_insert_raw_reddit_post(
        driver=driver,
        page_url=reddit_url,
        MINIO_CLIENT=MINIO_CLIENT,
        BUCKET_NAME=BUCKET_NAME,
        inserted_reddit_ids=inserted_reddit_post_ids,
        login=True,
        secrets=secrets,
    )
