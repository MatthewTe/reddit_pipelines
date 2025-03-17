import sqlalchemy as sa

from library.protobuff_types.reddit import reddit_post_pb2
from library.reddit_post_extraction_methods import (
    insert_reddit_posts_db,
    get_unique_posts,
    ExistingRedditPostsId,
)


def test_unique_protobuff_identification(example_reddit_protobuffs, test_secrets):

    # Inserting a reddit post as the already existed reddit post:
    reddit_posts = reddit_post_pb2.RedditPosts()
    existing_post = reddit_post_pb2.RedditPost(
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
    reddit_posts.posts.add().CopyFrom(existing_post)
    uploaded_reddit_post: int = insert_reddit_posts_db(existing_post, test_secrets)
    assert uploaded_reddit_post == 1

    existing_posts: list[ExistingRedditPostsId] = get_unique_posts(
        example_reddit_protobuffs, test_secrets
    )
    duplicate_ids: list[str] = [str(post["id"]) for post in existing_posts]
    assert duplicate_ids == ["ac88aacb-b29d-44d8-8a20-e0e41d4e8122"]

    unique_posts_to_ingest = [
        post for post in example_reddit_protobuffs.posts if post.id not in duplicate_ids
    ]
    assert [post.id for post in unique_posts_to_ingest] == [
        "8017eb45-9c1d-49e7-9c26-f0e7617d091f",
        "513bcbea-7f93-4ba4-8fd3-95245b316237",
    ]

    del reddit_posts.posts[:]
    reddit_posts.posts.extend(unique_posts_to_ingest)

    # This should be the remaining reddit posts to upload:
    assert [
        "8017eb45-9c1d-49e7-9c26-f0e7617d091f",
        "513bcbea-7f93-4ba4-8fd3-95245b316237",
    ] == [post.id for post in reddit_posts.posts]

    inserted_rows = 0
    for post in reddit_posts.posts:
        inserted_row = insert_reddit_posts_db(reddit_post=post, secrets=test_secrets)

        inserted_rows += inserted_row

    assert inserted_rows == 2

    sql_engine = sa.create_engine(test_secrets["psql_uri"])
    with sql_engine.connect() as conn, conn.begin():
        for post in example_reddit_protobuffs.posts:
            conn.execute(
                sa.text("DELETE FROM core.source WHERE core.source.id = :id"),
                {"id": post.id},
            )


# def test_unique_protobuff_ingestion_existing_identification():
#    pass
