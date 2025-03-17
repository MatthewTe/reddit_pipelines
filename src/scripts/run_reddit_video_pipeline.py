from selenium import webdriver
from selenium.webdriver.common.by import By

import uuid
from minio import Minio
from datetime import datetime, timezone
import pandas as pd
import argparse
from loguru import logger
import sys

from library.ingest_reddit_video import ingest_all_video_data
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

args = parser.parse_args()

secrets: Secrets = get_secrets(args.env_file)

ingest_all_video_data(secrets, args.post_ids)
