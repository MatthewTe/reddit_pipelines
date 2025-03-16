import pytest
import os
import sqlalchemy as sa

from library.config import Secrets
from library import reddit_post_pb2

from google.protobuf.message import Message


@pytest.fixture(scope="session", autouse=True)
def test_secrets() -> Secrets:
    os.environ["MINIO_URL"] = "127.0.0.1:9000"
    os.environ["MINIO_ACCESS_KEY"] = "test_access_key"
    os.environ["MINIO_SECRET_KEY"] = "test_secret_key"
    os.environ["REDDIT_USERNAME"] = "None"
    os.environ["REDDIT_PASSWORD"] = "None"
    os.environ[
        "PSQL_URI"
    ] = "postgresql://reddit_content_dev:test@127.0.0.1:5432/content_dev"

    return {
        "minio_url": os.environ.get("MINIO_URL"),
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY"),
        "reddit_username": os.environ.get("REDDIT_USERNAME"),
        "reddit_password": os.environ.get("REDDIT_PASSWORD"),
        "psql_uri": os.environ.get("PSQL_URI"),
    }


@pytest.fixture(scope="session")
def example_reddit_protobuffs() -> Message:

    reddit_posts = reddit_post_pb2.RedditPosts()

    post1 = reddit_post_pb2.RedditPost(
        id="ac88aacb-b29d-44d8-8a20-e0e41d4e8122",
        type="reddit_post",
        created_date=1712345678.0,
        fields=reddit_post_pb2.RedditPost.RedditPostFields(
            subreddit="CombatFootage",
            url="https://www.reddit.com/r/CombatFootage/comments/1jc301u/footage_claimed_to_show_the_first_use_of_the_new/",
            title='Footage claimed to show the first use of the new long range missile "Neptun" on the Tuapse Oil Refinery in Russia on March 14, 2025',
            static_downloaded_flag=False,
            screenshot_path="s3://reddit-posts/ac88aacb-b29d-44d8-8a20-e0e41d4e8122/screenshot.png",
            user=reddit_post_pb2.RedditUser(
                id="b5ace132-c213-4b2f-95b7-71e9dc98fad2",
                name="example_reddit_author",
                full_name="An example author reddit name",
            ),
            static_files=[
                reddit_post_pb2.StaticFileEntry(
                    type=reddit_post_pb2.StaticFileType.REDDIT_VIDEO,
                    id="240de578-f0f2-4dc8-9790-8c36d69ee3ac",
                ),
                reddit_post_pb2.StaticFileEntry(
                    type=reddit_post_pb2.StaticFileType.REDDIT_AUDIO,
                    id="7ba7f327-b26a-43bc-bf55-773884469123",
                ),
            ],
        ),
    )

    post2 = reddit_post_pb2.RedditPost(
        id="8017eb45-9c1d-49e7-9c26-f0e7617d091f",
        type="reddit_post",
        created_date=1712345678.0,
        fields=reddit_post_pb2.RedditPost.RedditPostFields(
            subreddit="CombatFootage",
            url="https://www.reddit.com/r/CombatFootage/comments/1jc5sff/saudi_air_force_targeted_a_ballistic_missile/",
            title=" Saudi Air Force targeted a ballistic missile being transported by the Houthis to another location in Yemen - Saada (date unknown)",
            static_downloaded_flag=True,
            screenshot_path="s3://reddit-posts/8017eb45-9c1d-49e7-9c26-f0e7617d091f/screenshot.png",
            user=reddit_post_pb2.RedditUser(
                id="b5ace132-c213-4b2f-95b7-71e9dc98fad2",
                name="example_reddit_author",
                full_name="An example author reddit name",
            ),
            static_files=[
                reddit_post_pb2.StaticFileEntry(
                    type=reddit_post_pb2.StaticFileType.REDDIT_VIDEO,
                    id="35c8896f-8384-4d51-be1f-ba87724a994a",
                ),
                reddit_post_pb2.StaticFileEntry(
                    type=reddit_post_pb2.StaticFileType.REDDIT_AUDIO,
                    id="06a80210-7d15-4798-abca-58e7fef61f75",
                ),
            ],
        ),
    )

    post3 = reddit_post_pb2.RedditPost(
        id="513bcbea-7f93-4ba4-8fd3-95245b316237",
        type="reddit_post",
        created_date=1712345678.0,
        fields=reddit_post_pb2.RedditPost.RedditPostFields(
            subreddit="CombatFootage",
            url="https://www.reddit.com/r/CombatFootage/comments/1jc301u/footage_claimed_to_show_the_first_use_of_the_new/",
            title='Footage claimed to show the first use of the new long range missile "Neptun" on the Tuapse Oil Refinery in Russia on March 14, 2025',
            static_downloaded_flag=False,
            screenshot_path="s3://reddit-posts/513bcbea-7f93-4ba4-8fd3-95245b316237/screenshot.png",
            user=reddit_post_pb2.RedditUser(
                id="b5ace132-c213-4b2f-95b7-71e9dc98fad2",
                name="example_reddit_author",
                full_name="An example author reddit name",
            ),
            static_files=[
                reddit_post_pb2.StaticFileEntry(
                    type=reddit_post_pb2.StaticFileType.REDDIT_VIDEO,
                    id="c445a0fc-c3e6-40ff-bea8-dce08fc30d3c",
                ),
                reddit_post_pb2.StaticFileEntry(
                    type=reddit_post_pb2.StaticFileType.REDDIT_AUDIO,
                    id="d18dfb65-6625-49ac-b83e-b1055ff49d86",
                ),
            ],
        ),
    )

    reddit_posts.posts.add().CopyFrom(post1)
    reddit_posts.posts.add().CopyFrom(post2)
    reddit_posts.posts.add().CopyFrom(post3)

    return reddit_posts
