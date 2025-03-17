from typing import TypedDict
import uuid
import pandas as pd
import json
import requests
import io
import random
import pprint
import time
import base64
import uuid
import sqlalchemy as sa
from datetime import datetime
from datetime import timezone
from loguru import logger
from minio import Minio
from minio.error import S3Error
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from google.protobuf.message import Message
from google.protobuf.json_format import MessageToJson, MessageToDict

from library.comments_extraction_methods import recursively_build_comment_creation_lst
from library.config import Secrets
from library.protobuff_types.reddit import reddit_post_pb2


class RedditPostDict(TypedDict):
    id: str
    subreddit: str
    url: str
    title: str
    static_downloaded_flag: bool
    screenshot_path: str
    json_path: str
    created_date: str
    static_root_url: str
    static_file_type: str


def get_post_message_from_element(post_element) -> Message:

    reddit_post_id = post_element.get_attribute("id")
    title = post_element.find_element(
        By.CSS_SELECTOR,
        f"#{reddit_post_id} > div:nth-child(5) > div:nth-child(1) > p:nth-child(1) > a:nth-child(1)",
    ).text
    id = str(uuid.uuid3(namespace=uuid.NAMESPACE_DNS, name=reddit_post_id))

    subreddit = post_element.get_attribute("data-subreddit")
    url = f"https://www.reddit.com{post_element.get_attribute('data-permalink')}"

    static_downloaded = False
    screenshot = f"{id}/screenshot.png"
    json = f"{id}/post.json"

    post_unix_timestamp = int(post_element.get_attribute("data-timestamp"))

    static_root_url = f"{id}/"
    static_file_type = post_element.get_attribute(f"data-kind")

    try:
        author_name = post_element.get_attribute("data-author")
        author_full_name = post_element.get_attribute("data-author-fullname")

        reddit_user: Message = reddit_post_pb2.RedditUser(
            id=str(uuid.uuid3(uuid.NAMESPACE_URL, author_full_name)),
            name=author_name,
            full_name=author_full_name,
        )
    except Exception as e:
        logger.error(f"error in trying to extract author from post {str(e)}")
        reddit_user: Message = reddit_post_pb2.RedditUser(
            id=str(uuid.uuid3(uuid.NAMESPACE_URL, "not_found")),
            name="not_found",
            full_name="not_found",
        )

    post = reddit_post_pb2.RedditPost(
        id=id,
        type="reddit_post",
        created_date=post_unix_timestamp,
        fields=reddit_post_pb2.RedditPost.RedditPostFields(
            subreddit=subreddit,
            url=url,
            title=title,
            static_downloaded_flag=static_downloaded,
            screenshot_path=screenshot,
            json_file_path=json,
            post_created_date=post_unix_timestamp,
            static_root_url=static_root_url,
            user=reddit_user,
        ),
    )

    if static_file_type == "video":
        static_file_entry = reddit_post_pb2.StaticFileEntry(
            type=reddit_post_pb2.StaticFileType.REDDIT_VIDEO, id="NULL"
        )
        post.fields.static_files.append(static_file_entry)

    return post


class RedditUserDict(TypedDict):
    id: str
    author_name: str
    author_full_name: str


def get_author_message_from_element(post_element) -> Message:

    try:
        author_name = post_element.get_attribute("data-author")
        author_full_name = post_element.get_attribute("data-author-fullname")

        reddit_user: Message = reddit_post_pb2.RedditUser(
            id=str(uuid.uuid3(uuid.NAMESPACE_URL, author_full_name)),
            name=author_name,
            full_name=author_full_name,
        )
    except Exception as e:
        logger.error(f"error in trying to extract author from post {str(e)}")
        reddit_user: Message = reddit_post_pb2.RedditUser(
            id=str(uuid.uuid3(uuid.NAMESPACE_URL, "not_found")),
            name="not_found",
            full_name="not_found",
        )
    return reddit_user


class RedditCommentDict(TypedDict):
    reddit_post_id: str
    comment_id: str
    comment_body: str
    associated_user: RedditUserDict
    posted_timestamp: str
    replies: dict[str, dict]


class RedditCommentAttachmentDict(TypedDict):
    reddit_post: RedditPostDict
    attached_comments: list[dict]


def get_comments_from_json(
    post: RedditPostDict, json_bytes_stream: io.BytesIO
) -> RedditCommentAttachmentDict:
    try:
        json_bytes_stream.seek(0)

        row_reddit_json = json.loads(json_bytes_stream.read())
        comment_content = row_reddit_json[1]["data"]["children"]

        post_comment_dicts: list[RedditCommentDict] = []
        for comment_json in comment_content:
            recursively_build_comment_creation_lst(
                output_lst=post_comment_dicts, post=post, comment_json_obj=comment_json
            )

        return post_comment_dicts

    except Exception as e:
        logger.error(
            f"Unable to recusrively extract all comments from comments json: {str(e.with_traceback(None))}"
        )
        return None


def get_post_json(driver, url) -> io.BytesIO | None:

    time.sleep(random.uniform(1.5, 3.5))

    try:

        driver.get(f"{url}.json")

        json_element = driver.find_element(By.XPATH, "/html/body/pre")
        json_dict = json_element.get_attribute("innerText")

        json_bytes_stream = io.BytesIO()
        json_bytes_stream.write(json_dict.encode())

        time.sleep(random.uniform(1.5, 3.5))

        return json_bytes_stream

    except requests.HTTPError as err:
        logger.error(
            f"""
            Unable to get the json representation of the post {url} \n
            - error: {str(err)}
        """
        )
        return None


def take_post_screenshot(driver, url) -> bytes:

    try:
        driver.get(url)
        driver.implicitly_wait(4000)

        time.sleep(random.randint(2, 5))

        screenshot_base64 = driver.get_screenshot_as_base64()
        screenshot_bytes: bytes = base64.b64decode(screenshot_base64)

        screenshot_bytes_stream = io.BytesIO()
        screenshot_bytes_stream.write(screenshot_bytes)

        return screenshot_bytes_stream
    except Exception as e:
        logger.error(
            f"""Error in trying to take a screenshot of page {url}
        - error {str(e)}
        """
        )
        return None


def insert_static_file_to_blob(
    memory_buffer: io.BytesIO,
    bucket_name: str,
    full_filepath: str,
    content_type: str,
    minio_client: Minio,
):

    try:

        memory_buffer.seek(0)

        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=full_filepath,
            data=memory_buffer,
            length=memory_buffer.getbuffer().nbytes,
            content_type=content_type,
        )
        return full_filepath

    except S3Error as exc:
        print("Error in inserting file to blob: ", exc)
        return None


class RedditPostCreationResult(TypedDict):
    parent_post: RedditPostDict
    attached_user: RedditUserDict


def insert_reddit_posts_db(reddit_post: Message, secrets: Secrets) -> int:
    psql_engine = sa.create_engine(secrets["psql_uri"])

    with psql_engine.connect() as conn, conn.begin():
        insert_query = sa.text(
            """
            INSERT INTO core.source (id, type, created_date, fields)
            VALUES (:id, :type, :created_date, :fields);
            """
        )

        posts_to_insert = {
            "id": reddit_post.id,
            "type": reddit_post.type,
            "created_date": datetime.fromtimestamp(
                reddit_post.created_date / 1000, tz=timezone.utc
            ),
            "fields": json.dumps(MessageToDict(reddit_post.fields)),
        }

        result = conn.execute(insert_query, posts_to_insert)

        logger.info(f"Inserted {result.rowcount} posts to tables")
        return result.rowcount


def attach_reddit_post_comments(
    attached_comment_dict: RedditCommentAttachmentDict, secrets: Secrets
) -> RedditCommentAttachmentDict | None:

    try:
        attached_comment_response = requests.post(
            f"{secrets['neo4j_url']}/v1/api/run_query", json=attached_comment_dict
        )
        attached_comment_response.raise_for_status()
        attached_comments: RedditCommentAttachmentDict = (
            attached_comment_response.json()
        )
        return attached_comments
    except requests.HTTPError as e:
        logger.error(
            f"""Unable to create comments and attach them to reddit post - {str(e)} \n
            - Post: {attached_comment_dict['reddit_post']} \n
            - Comments {attached_comment_dict['attached_comments']}
        """
        )
        return None


class ExistingRedditPostsId(TypedDict):
    id: str


def get_unique_posts(
    reddit_posts: Message, secrets: Secrets
) -> list[ExistingRedditPostsId]:
    PSQL_Engine: sa.engine.Engine = sa.create_engine(secrets["psql_uri"])
    with PSQL_Engine.connect() as conn, conn.begin():

        get_unique_id_query = sa.text(
            """
            SELECT id
            FROM core.source source
            WHERE source.id = ANY(:ids)
            AND type = 'reddit_post'
            """
        ).bindparams(sa.bindparam("ids", type_=ARRAY(UUID)))

        existing_id_results = conn.execute(
            get_unique_id_query, {"ids": [post.id for post in reddit_posts.posts]}
        )
        existing_ids_dict = existing_id_results.mappings().all()
        return existing_ids_dict


def recursive_insert_raw_reddit_post(
    driver: webdriver.Chrome,
    page_url: str,
    MINIO_CLIENT,
    BUCKET_NAME: str,
    secrets: Secrets,
    inserted_reddit_ids: list[str],
    login: bool = False,
):
    driver.get(page_url)
    driver.implicitly_wait(3000)

    # Loging in:
    if login:
        login_button = driver.find_element(
            By.XPATH, "//a[@class='login-required login-link']"
        )
        login_button.click()

        time.sleep(random.randint(1, 3))

        login_input = driver.find_element(By.ID, "login-username")
        password_input = driver.find_element(By.ID, "login-password")

        login_input.send_keys(secrets["reddit_username"])
        password_input.send_keys(secrets["reddit_password"])

        input("Press any key to resume...")

    time.sleep(random.randint(3, 5))

    posts_site_table = driver.find_element(By.ID, "siteTable")

    next_button_results: list = driver.find_elements(
        By.XPATH, "//span[@class='next-button']"
    )
    if len(next_button_results) == 0:
        next_button_url = None
    else:
        next_button_url = (
            next_button_results[0].find_element(By.TAG_NAME, "a").get_attribute("href")
        )

    logger.info(f"Next url for next page: {next_button_url}")

    all_posts_on_page = posts_site_table.find_elements(
        By.XPATH, "//div[@data-context='listing']"
    )

    reddit_posts: Message = reddit_post_pb2.RedditPosts()
    for post_element in all_posts_on_page:
        reddit_post_message: Message = get_post_message_from_element(post_element)
        pprint.pprint(reddit_post_message)
        reddit_posts.posts.append(reddit_post_message)

    logger.info(
        f"Found a total of {len(reddit_posts.posts)} posts from page {page_url}"
    )

    existing_posts: list[ExistingRedditPostsId] = get_unique_posts(
        reddit_posts, secrets
    )
    logger.info(f"Response from existing post db query: \n")
    pprint.pprint(existing_posts)

    duplicate_ids: list[str] = [str(post["id"]) for post in existing_posts]

    unique_posts_to_ingest: Message = reddit_post_pb2.RedditPosts()
    unique_posts_to_ingest.posts.extend(
        [post for post in reddit_posts.posts if post.id not in duplicate_ids]
    )

    logger.info(
        f"Found {len(reddit_posts.posts)} unique posts that are not in the reddit database"
    )

    # Actually inserting the post data into the database:
    if len(reddit_posts.posts) == 0:
        logger.info("No unique posts found from reddit page. Exiting...")
        return

    logger.info(f"Beginning to process unique posts")

    ids_successfully_uploaded: list[str] = []

    for post in unique_posts_to_ingest.posts:

        logger.info(f"Trying to take screenshot for {post.fields.url}")
        screenshot_stream: io.BytesIO | None = take_post_screenshot(
            driver, post.fields.url
        )
        if screenshot_stream is None:
            logger.error(
                f"""Screenshot bytes stream returned as none with error. Not inserting post {post.id} \n
            - post {pprint.pprint(post)}
            """
            )
            continue

        logger.info(f"Extracting json representation of post {post.fields.url}")
        json_stream: io.BytesIO | None = get_post_json(driver, post.fields.url)
        if json_stream is None:
            logger.error(
                f"""json response bytes stream returned as none with error. Not inserting post {post.id} \n
            - post {pprint.pprint(post)}
            """
            )
            continue

        uploaded_screenshot_filepath: str | None = insert_static_file_to_blob(
            memory_buffer=screenshot_stream,
            bucket_name=BUCKET_NAME,
            full_filepath=post.fields.screenshot_path,
            content_type="image/png",
            minio_client=MINIO_CLIENT,
        )

        uploaded_json_filepath: str | None = insert_static_file_to_blob(
            memory_buffer=json_stream,
            bucket_name=BUCKET_NAME,
            full_filepath=post.fields.json_file_path,
            content_type="application/json",
            minio_client=MINIO_CLIENT,
        )

        if uploaded_screenshot_filepath is None or uploaded_json_filepath is None:
            logger.error(
                f"Uploaded screenshot or json filepath is None so there was an error in inserting a static file to blob"
            )
            continue
        else:
            logger.info(
                f"Sucessfully inserted screenshot and json to blob storage: \n - sreenshot: {uploaded_json_filepath} \n -json post: {uploaded_json_filepath}"
            )

        post.fields.screenshot_path = uploaded_screenshot_filepath
        post.fields.json_file_path = uploaded_json_filepath

        logger.info(f"Inserting all {len(post.id)} to database")
        uploaded_post_count: int = insert_reddit_posts_db(
            reddit_post=post, secrets=secrets
        )
        if uploaded_post_count == 1:
            logger.info(f"Uploaded {post.id} to the database")
            ids_successfully_uploaded.append(post.id)

        else:
            logger.warning(
                f"Error in uploading post {post.id} to database. Not marking the post as uploaded"
            )
            continue

    if len(ids_successfully_uploaded) != len(unique_posts_to_ingest.posts):
        logger.error(
            f"Error in uploading all of the posts to the database - {len(unique_posts_to_ingest.posts)} Posts ingested should have been {[post.id for post in unique_posts_to_ingest]} but only inserted {ids_successfully_uploaded}"
        )
        return

    inserted_reddit_ids.extend(
        post.id
        for post in unique_posts_to_ingest.posts
        if post.id in ids_successfully_uploaded
    )

    if next_button_url is None:
        logger.info(
            f"next url for page {page_url} was extracted to be None. Exiting recursive call"
        )
        return

    logger.info(
        f"next url for page {page_url} extracted as {next_button_url} - continuing to call recursively"
    )

    recursive_insert_raw_reddit_post(
        driver=driver,
        page_url=next_button_url,
        MINIO_CLIENT=MINIO_CLIENT,
        BUCKET_NAME=BUCKET_NAME,
        inserted_reddit_ids=inserted_reddit_ids,
        login=False,
        secrets=secrets,
    )
