import pytest
import sqlalchemy as sa
import pprint

from library.protobuff_types.reddit import reddit_post_pb2
from library.reddit_post_extraction_methods import insert_reddit_posts_db
from library.config import Secrets

from google.protobuf.json_format import MessageToJson, MessageToDict
from google.protobuf.message import Message


def test_protobuff_types():

    reddit_posts = reddit_post_pb2.RedditPosts()

    post1 = reddit_post_pb2.RedditPost(
        id="ac88aacb-b29d-44d8-8a20-e0e41d4e8122",
        type="RedditPost",
        created_date=1712345678.0,
        fields=reddit_post_pb2.RedditPost.RedditPostFields(
            subreddit="CombatFootage",
            url="https://www.reddit.com/r/CombatFootage/comments/1jc301u/footage_claimed_to_show_the_first_use_of_the_new/",
            title='Footage claimed to show the first use of the new long range missile "Neptun" on the Tuapse Oil Refinery in Russia on March 14, 2025',
            static_downloaded_flag=False,
            screenshot_path="s3://reddit-posts/ac88aacb-b29d-44d8-8a20-e0e41d4e8122/screenshot.png",
            json_file_path="example_json_path",
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
        type="RedditPost",
        created_date=1712345678.0,
        fields=reddit_post_pb2.RedditPost.RedditPostFields(
            subreddit="CombatFootage",
            url="https://www.reddit.com/r/CombatFootage/comments/1jc5sff/saudi_air_force_targeted_a_ballistic_missile/",
            title=" Saudi Air Force targeted a ballistic missile being transported by the Houthis to another location in Yemen - Saada (date unknown)",
            static_downloaded_flag=True,
            screenshot_path="s3://reddit-posts/8017eb45-9c1d-49e7-9c26-f0e7617d091f/screenshot.png",
            json_file_path="example_json_path",
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
    reddit_posts.posts.add().CopyFrom(post1)
    reddit_posts.posts.add().CopyFrom(post2)


def test_correct_reddit_post_seralization(example_reddit_protobuffs: Message):

    for post in example_reddit_protobuffs.posts:
        pprint.pprint(MessageToDict(post, preserving_proto_field_name=True))


# @pytest.mark.skip
def test_insert_protobuff_data_to_db(
    example_reddit_protobuffs: Message, test_secrets: Secrets
):

    inserted_rows = 0
    for post in example_reddit_protobuffs.posts:
        inserted_rows += insert_reddit_posts_db(reddit_post=post, secrets=test_secrets)

    assert inserted_rows == 3

    sql_engine = sa.create_engine(test_secrets["psql_uri"])
    with sql_engine.connect() as conn, conn.begin():
        for post in example_reddit_protobuffs.posts:
            conn.execute(
                sa.text("DELETE FROM core.source WHERE core.source.id = :id"),
                {"id": post.id},
            )
