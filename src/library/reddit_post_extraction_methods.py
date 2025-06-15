from typing import TypedDict
import uuid
import json
import requests
import io
import random
import pprint
import time
import base64
import uuid
from loguru import logger
from selenium import webdriver
from selenium.webdriver.common.by import By

from library.io_interfaces.db_io import DatabaseInterface
from library.io_interfaces.filestore_io import FileInterface
from library.types import RedditPostDict, RedditUserDict


def get_post_message_from_element(post_element) -> RedditPostDict:

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
        reddit_user: RedditUserDict = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, author_full_name)),
            "name": author_name,
            "full_name": author_full_name,
        }

    except Exception as e:
        logger.error(f"error in trying to extract author from post {str(e)}")

        reddit_user: RedditUserDict = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, "not_found")),
            "name": "not_found",
            "full_name": "not_found",
        }

    post: RedditPostDict = {
        "id": id,
        "created_date": post_unix_timestamp,
        "type": "reddit_post",
        "fields": {
            "subreddit": subreddit,
            "url": url,
            "title": title,
            "static_downloaded_flag": static_downloaded,
            "screenshot_path": screenshot,
            "json_file_path": json,
            "post_created_date": post_unix_timestamp,
            "static_root_url": static_root_url,
            "user": reddit_user,
            "static_files": [],
        },
    }

    if static_file_type == "video":
        post["fields"]["static_files"].append({"id": "NULL", "type": "video"})

    return post


def get_author_message_from_element(post_element) -> RedditUserDict:

    try:
        author_name = post_element.get_attribute("data-author")
        author_full_name = post_element.get_attribute("data-author-fullname")

        reddit_user: RedditUserDict = dict(
            id=str(uuid.uuid3(uuid.NAMESPACE_URL, author_full_name)),
            name=author_name,
            full_name=author_full_name,
        )

    except Exception as e:
        logger.error(f"error in trying to extract author from post {str(e)}")
        reddit_user: RedditUserDict = dict(
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


def get_post_json_response(driver, url: str) -> io.BytesIO | None:

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


def take_post_screenshot(driver, url: str) -> bytes:

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


def recursive_insert_raw_reddit_post(
    driver: webdriver.Chrome,
    page_url: str,
    config: dict,
    inserted_reddit_ids: list[str],
    file_io: FileInterface,
    database_io: DatabaseInterface,
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

        login_input.send_keys(config["reddit_username"])
        password_input.send_keys(config["reddit_password"])

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

    reddit_posts: list[RedditPostDict] = []
    for post_element in all_posts_on_page:
        reddit_post_message: RedditPostDict = get_post_message_from_element(
            post_element
        )
        pprint.pprint(reddit_post_message)
        reddit_posts.append(reddit_post_message)

    logger.info(f"Found a total of {len(reddit_posts)} posts from page {page_url}")

    existing_posts: list[str] | None = database_io.get_unique_posts(
        ids=[post["id"] for post in reddit_posts],
        db_engine=config["db_engine"],
        config=config,
    )
    logger.info(f"Response from existing post db query: \n")
    pprint.pprint(existing_posts)

    assert not existing_posts is None, "Exiting after error w/ unique posts query"

    duplicate_ids: list[str] = [str(id) for id in existing_posts]

    unique_posts_to_ingest: list[RedditPostDict] = []
    unique_posts_to_ingest.extend(
        [post for post in reddit_posts if post["id"] not in duplicate_ids]
    )

    logger.info(
        f"Found {len(unique_posts_to_ingest)} unique posts that are not in the reddit database"
    )

    # Actually inserting the post data into the database:
    if len(unique_posts_to_ingest) == 0:
        logger.info("No unique posts found from reddit page. Exiting...")
        return

    logger.info(f"Beginning to process unique posts")

    ids_successfully_uploaded: list[str] = []

    for post in unique_posts_to_ingest:

        logger.info(f"Trying to take screenshot for {post['fields']['url']}")
        screenshot_stream: io.BytesIO | None = take_post_screenshot(
            driver, post["fields"]["url"]
        )
        if screenshot_stream is None:
            logger.error(
                f"""Screenshot bytes stream returned as none with error. Not inserting post {post['id']} \n
            - post {pprint.pprint(post)}
            """
            )
            continue

        logger.info(f"Extracting json representation of post {post['fields']['url']}")
        json_stream: io.BytesIO | None = get_post_json_response(
            driver, post["fields"]["url"]
        )
        if json_stream is None:
            logger.error(
                f"""json response bytes stream returned as none with error. Not inserting post {post['id']} \n
            - post {pprint.pprint(post)}
            """
            )
            continue

        config["content_type"] = "image/png"
        uploaded_screenshot_filepath: str | None = file_io.upload_file(
            screenshot_stream,
            dir_name=config["root_dir_name"],
            filepath=post["fields"]["screenshot_path"],
            config=config,
        )

        config["content_type"] = "application/json"
        uploaded_json_filepath: str | None = file_io.upload_file(
            json_stream,
            dir_name=config["root_dir_name"],
            filepath=post["fields"]["json_file_path"],
            config=config,
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

        post["fields"]["screenshot_path"] = uploaded_screenshot_filepath
        post["fields"]["json_file_path"] = uploaded_json_filepath

        logger.info(f"Inserting post {post['id']} to database")
        uploaded_post_response: str | None = database_io.insert_reddit_posts_db(
            reddit_post=post, db_engine=config["db_engine"], config=config
        )

        if uploaded_post_response is not None:
            logger.info(f"Uploaded {post['id']} to the database")
            ids_successfully_uploaded.append(post["id"])

        else:
            logger.warning(
                f"Error in uploading post {post['id']} to database. Not marking the post as uploaded"
            )
            continue

    if len(ids_successfully_uploaded) != len(unique_posts_to_ingest):
        logger.error(
            f"Error in uploading all of the posts to the database - {len(unique_posts_to_ingest)}  \
                Posts ingested should have been {[post['id'] for post in unique_posts_to_ingest]} \
                but only inserted {ids_successfully_uploaded}"
        )
        return

    inserted_reddit_ids.extend(
        post["id"]
        for post in unique_posts_to_ingest
        if post["id"] in ids_successfully_uploaded
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
        config=config,
        inserted_reddit_ids=inserted_reddit_ids,
        file_io=file_io,
        database_io=database_io,
        login=False,
    )
