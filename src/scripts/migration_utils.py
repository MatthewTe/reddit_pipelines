from library.protobuff_types.reddit import reddit_post_pb2

import uuid
from loguru import logger
from google.protobuf.message import Message


def extract_post_from_dict(input_json, id):

    main_data_component = input_json[0]["data"]["children"][0]["data"]
    try:
        reddit_user: Message = reddit_post_pb2.RedditUser(
            id=str(
                uuid.uuid3(uuid.NAMESPACE_URL, main_data_component["author_fullname"])
            ),
            name=main_data_component["author"],
            full_name=main_data_component["author_fullname"],
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
        created_date=main_data_component["created_utc"] * 1000,
        fields=reddit_post_pb2.RedditPost.RedditPostFields(
            subreddit=main_data_component["subreddit"],
            url=f"https://www.reddit.com/{main_data_component['permalink']}",
            title=main_data_component["title"],
            static_downloaded_flag=True,
            screenshot_path="",
            json_file_path=f"{id}/post.json",
            post_created_date=main_data_component["created_utc"],
            static_root_url=f"{id}/",
            user=reddit_user,
        ),
    )
    return post
