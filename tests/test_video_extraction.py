import pytest
import json
import sqlalchemy as sa
import pprint

from library.protobuff_types.reddit import reddit_post_pb2
from library.reddit_post_extraction_methods import insert_reddit_posts_db
from library.ingest_reddit_video import (
    get_reddit_video_posts,
    parse_video_from_mpd_document,
    update_reddit_post_video_content,
    upload_mpd_reddit_record,
    RedditVideoInfoDict,
    ParsedMPDResult,
)
from library.config import Secrets

from google.protobuf.message import Message


def test_reddit_video_identification(
    example_reddit_protobuffs: Message, test_secrets: Secrets
):

    for post in example_reddit_protobuffs.posts:
        insert_reddit_posts_db(reddit_post=post, secrets=test_secrets)

    # Data has been inserted - pass in list of all ids identify the video posts that have no been processed:
    reddit_post_ids = [post.id for post in example_reddit_protobuffs.posts]

    video_posts_to_ingest: list[dict] = get_reddit_video_posts(
        reddit_post_ids, test_secrets
    )

    assert len(video_posts_to_ingest) == 2

    assert set([str(post["id"]) for post in video_posts_to_ingest]) == set(
        ["513bcbea-7f93-4ba4-8fd3-95245b316237", "8017eb45-9c1d-49e7-9c26-f0e7617d091f"]
    )

    sql_engine = sa.create_engine(test_secrets["psql_uri"])
    with sql_engine.connect() as conn, conn.begin():
        for post in example_reddit_protobuffs.posts:
            conn.execute(
                sa.text("DELETE FROM core.source WHERE core.source.id = :id"),
                {"id": post.id},
            )


@pytest.mark.skip
def test_mpd_file_extraction_parsing():
    with open("./data/example_post.json", "r") as f:
        decoded_json = json.load(f)

    response_json = decoded_json[0]["data"]
    post_data = response_json["children"][0]["data"]

    media_dict = post_data.get("secure_media", None)
    if media_dict is not None:
        reddit_video: RedditVideoInfoDict = media_dict.get("reddit_video", None)

        parsed_mpd_result: ParsedMPDResult = parse_video_from_mpd_document(
            reddit_video, post_data
        )


def test_updating_video_id(example_reddit_protobuffs: Message, test_secrets: Secrets):

    for post in example_reddit_protobuffs.posts:
        insert_reddit_posts_db(reddit_post=post, secrets=test_secrets)

    # Data has been inserted - pass in list of all ids identify the video posts that have no been processed:
    reddit_post_ids = [post.id for post in example_reddit_protobuffs.posts]

    video_posts_to_ingest: list[dict] = get_reddit_video_posts(
        reddit_post_ids, test_secrets
    )

    assert len(video_posts_to_ingest) == 2
    assert set([str(post["id"]) for post in video_posts_to_ingest]) == set(
        ["513bcbea-7f93-4ba4-8fd3-95245b316237", "8017eb45-9c1d-49e7-9c26-f0e7617d091f"]
    )

    updated_row: int = update_reddit_post_video_content(
        post_id="8017eb45-9c1d-49e7-9c26-f0e7617d091f",
        video_id="322c5aa5-ee97-49cf-9479-92650bbd15e2",
        full_video_path="322c5aa5-ee97-49cf-9479-92650bbd15e2/Example_Video_Stream.mpd",
        secrets=test_secrets,
    )

    assert updated_row == 1

    sql_engine = sa.create_engine(test_secrets["psql_uri"])
    with sql_engine.connect() as conn, conn.begin():

        get_updated_reddit_post = sa.text("SELECT * FROM core.source WHERE id = :id")
        updated_post_result = conn.execute(
            get_updated_reddit_post, {"id": "8017eb45-9c1d-49e7-9c26-f0e7617d091f"}
        )

        post_dict: dict = dict(updated_post_result.mappings().first())

        pprint.pprint(post_dict["fields"]["staticFiles"])

        updated_static_file_content = [
            static_file
            for static_file in post_dict["fields"]["staticFiles"]
            if static_file["type"] == "REDDIT_VIDEO"
        ][0]
        assert updated_static_file_content == {
            "id": "322c5aa5-ee97-49cf-9479-92650bbd15e2",
            "path": "322c5aa5-ee97-49cf-9479-92650bbd15e2/Example_Video_Stream.mpd",
            "type": "REDDIT_VIDEO",
        }

        assert str(post_dict["id"]) == "8017eb45-9c1d-49e7-9c26-f0e7617d091f"
        assert post_dict["fields"]["static_downloaded_flag"] == True

        for post in example_reddit_protobuffs.posts:
            conn.execute(
                sa.text("DELETE FROM core.source WHERE core.source.id = :id"),
                {"id": post.id},
            )


def test_uploading_reddit_video_content(
    example_reddit_protobuffs: Message,
    reddit_video_content: Message,
    test_secrets: Secrets,
):
    for post in example_reddit_protobuffs.posts:
        insert_reddit_posts_db(reddit_post=post, secrets=test_secrets)

    # Data has been inserted - pass in list of all ids identify the video posts that have no been processed:
    reddit_post_ids = [post.id for post in example_reddit_protobuffs.posts]

    video_posts_to_ingest: list[dict] = get_reddit_video_posts(
        reddit_post_ids, test_secrets
    )

    assert len(video_posts_to_ingest) == 2
    assert set([str(post["id"]) for post in video_posts_to_ingest]) == set(
        ["513bcbea-7f93-4ba4-8fd3-95245b316237", "8017eb45-9c1d-49e7-9c26-f0e7617d091f"]
    )

    # Get the post that we want to upload the video and attach it too:
    reddit_post_w_video: dict = [
        post
        for post in video_posts_to_ingest
        if str(post["id"] == "8017eb45-9c1d-49e7-9c26-f0e7617d091f")
    ][0]

    print(reddit_post_w_video)
    print("Uploading reddit video content")
    pprint.pprint(reddit_video_content)

    uploaded_video_stream: int = upload_mpd_reddit_record(
        reddit_video_content, test_secrets
    )
    assert uploaded_video_stream == 1
    print(f"Updating the source row now that the reddit content row has been uploaded")

    updated_row: int = update_reddit_post_video_content(
        post_id="8017eb45-9c1d-49e7-9c26-f0e7617d091f",
        video_id=reddit_video_content.id,
        full_video_path=reddit_video_content.storage_path,
        secrets=test_secrets,
    )
    assert updated_row == 1

    sql_engine = sa.create_engine(test_secrets["psql_uri"])
    with sql_engine.connect() as conn, conn.begin():

        get_updated_reddit_post = sa.text("SELECT * FROM core.source WHERE id = :id")
        updated_post_result = conn.execute(
            get_updated_reddit_post, {"id": "8017eb45-9c1d-49e7-9c26-f0e7617d091f"}
        )

        post_dict: dict = dict(updated_post_result.mappings().first())

        pprint.pprint(post_dict["fields"]["staticFiles"])

        updated_static_file_content = [
            static_file
            for static_file in post_dict["fields"]["staticFiles"]
            if static_file["type"] == "REDDIT_VIDEO"
        ][0]
        assert updated_static_file_content == {
            "id": reddit_video_content.id,
            "path": reddit_video_content.storage_path,
            "type": "REDDIT_VIDEO",
        }

        assert str(post_dict["id"]) == "8017eb45-9c1d-49e7-9c26-f0e7617d091f"
        assert post_dict["fields"]["static_downloaded_flag"] == True

        conn.execute(
            sa.text(
                "DELETE FROM core.content WHERE id = 'e4c295c8-ab9a-4b5a-96eb-31c8b2310fd2'"
            )
        )

        for post in example_reddit_protobuffs.posts:
            conn.execute(
                sa.text("DELETE FROM core.source WHERE core.source.id = :id"),
                {"id": post.id},
            )


def test_validating_updated_static_video_content(
    example_reddit_protobuffs: Message, test_secrets: Secrets
):
    for post in example_reddit_protobuffs.posts:
        insert_reddit_posts_db(reddit_post=post, secrets=test_secrets)

    reddit_post_ids = [post.id for post in example_reddit_protobuffs.posts]
    video_posts_to_ingest: list[dict] = get_reddit_video_posts(
        reddit_post_ids, test_secrets
    )

    assert len(video_posts_to_ingest) == 2
    assert set([str(post["id"]) for post in video_posts_to_ingest]) == set(
        ["513bcbea-7f93-4ba4-8fd3-95245b316237", "8017eb45-9c1d-49e7-9c26-f0e7617d091f"]
    )

    # Update one reddit post:
    updated_first_row: int = update_reddit_post_video_content(
        post_id="8017eb45-9c1d-49e7-9c26-f0e7617d091f",
        video_id="e4c295c8-ab9a-4b5a-96eb-31c8b2310fd2",
        full_video_path="e4c295c8-ab9a-4b5a-96eb-31c8b2310fd2/Example_Video_Stream.mpd",
        secrets=test_secrets,
    )

    assert updated_first_row == 1

    # Now query the database again to see if the reddit post has been sucessfully updated:
    updated_posts_to_ingest: list[dict] = get_reddit_video_posts(
        reddit_post_ids, test_secrets
    )
    assert len(updated_posts_to_ingest) == 1
    assert set([str(post["id"]) for post in updated_posts_to_ingest]) == set(
        ["513bcbea-7f93-4ba4-8fd3-95245b316237"]
    )

    # Now update the next reddit post:
    updated_second_row: int = update_reddit_post_video_content(
        post_id="513bcbea-7f93-4ba4-8fd3-95245b316237",
        video_id="e6ac5703-701a-4607-aca5-dac8ad7564d8",
        full_video_path="e6ac5703-701a-4607-aca5-dac8ad7564d8/Example_Next_Video_Stream.mpd",
        secrets=test_secrets,
    )
    assert updated_second_row == 1

    second_updated_posts_to_ingest: list[dict] = get_reddit_video_posts(
        reddit_post_ids, test_secrets
    )
    assert len(second_updated_posts_to_ingest) == 0
    assert set(second_updated_posts_to_ingest) == set([])

    sql_engine = sa.create_engine(test_secrets["psql_uri"])
    with sql_engine.connect() as conn, conn.begin():
        for post in example_reddit_protobuffs.posts:
            conn.execute(
                sa.text("DELETE FROM core.source WHERE core.source.id = :id"),
                {"id": post.id},
            )
