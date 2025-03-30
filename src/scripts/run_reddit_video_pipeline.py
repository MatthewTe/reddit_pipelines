from selenium import webdriver
from selenium.webdriver.common.by import By

import uuid
from minio import Minio
from datetime import datetime, timezone
import pandas as pd
import argparse
from loguru import logger
import sys

from library.ingest_reddit_video import (
    get_all_reddit_video_posts,
    ingest_all_video_data,
)
from library.config import (
    get_secrets,
    Secrets,
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "-e",
    "--env_file",
    help="The path to the environment file used to load all of the secrets",
)
parser.add_argument(
    "-i",
    "--post_ids",
    nargs="+",
    help="The Reddit post ids that have already been uploaded to the database to process and upload videos of to the db",
)

parser.add_argument(
    "-s",
    "--setting",
    help="If a manual list of post ids isn't provided via --post_ids then this argument determines how the IDs are extracted from the database to ingest videos",
    default="manual",
)

args = parser.parse_args()

secrets: Secrets = get_secrets(args.env_file)

if __name__ == "__main__":
    match args.setting:
        case "manual":
            logger.info(
                f"Ingesting all post ids with manually provided ids {args.post_ids}"
            )
            ingest_all_video_data(secrets, args.post_ids)

        case "full_ingest":
            all_posts = get_all_reddit_video_posts(secrets)
            print(all_posts)
            logger.info(
                f"Performing dumb full video content ingest for {len(all_posts)} posts"
            )
            ingest_all_video_data(secrets, [post["id"] for post in all_posts])

        case default:
            logger.error(f"Not a supported upload type/setting: {args.setting}")
