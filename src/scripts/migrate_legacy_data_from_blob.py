"A script that migrate the legacy minio data into the new PSQL schema"

from minio import Minio
import json
import argparse
from loguru import logger
import uuid

from library.reddit_post_extraction_methods import insert_reddit_posts_db
from library.ingest_reddit_video import upload_mpd_reddit_record

from library.config import (
    get_secrets,
    Secrets,
)
from library.protobuff_types.reddit import reddit_post_pb2
from library.protobuff_types import core_content_pb2
from scripts.migration_utils import extract_post_from_dict

parser = argparse.ArgumentParser()
parser.add_argument(
    "-e",
    "--env_file",
    help="The path to the environment file used to load all of the secrets",
)

args = parser.parse_args()
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

    existing_objects = MINIO_CLIENT.list_objects(BUCKET_NAME)

    for obj in existing_objects:
        if obj.is_dir:
            objects_in_post_dir = MINIO_CLIENT.list_objects(
                BUCKET_NAME, prefix=obj.object_name
            )

            post_id = obj.object_name.replace("/", "")
            objs_filenames = [obj.object_name for obj in objects_in_post_dir]

            # Isolate JSON filename:
            json_filename = objs_filenames.pop(
                objs_filenames.index(f"{post_id}/post.json")
            )
            json_response = MINIO_CLIENT.get_object(BUCKET_NAME, json_filename)
            post_dict: dict = json.loads(json_response.data)
            post = extract_post_from_dict(post_dict, post_id)

            screenshot_path = objs_filenames.pop(
                objs_filenames.index(f"{post_id}/screenshot.png")
            )
            post.fields.screenshot_path = screenshot_path

            try:
                MINIO_CLIENT.get_object(
                    BUCKET_NAME, object_name=f"{post_id}/Graph_DASH.mpd"
                )
            except Exception as e:
                logger.error(e)
                assert (
                    insert_reddit_posts_db(reddit_post=post, secrets=secrets) == 1
                ), logger.error(
                    f"Error in uploading {post.id} migrated post to database"
                )
                continue

            # id/Origin_DASH.mpd
            # id/{period}_video.mp4
            # id/{period}_audio.mp4
            # id/Video_DASH.mpd comes from GRAPH_DASH.mpd

            # Video content:
            video_stream_id: str = str(
                uuid.uuid3(uuid.NAMESPACE_URL, f"{post_id}/Graph_DASH.mpd")
            )

            video_stream_content = reddit_post_pb2.RedditVideoContent(
                id=video_stream_id,
                source=post_id,
                type=core_content_pb2.CoreContentTypes.VIDEO_DASH_STREAM,
                created_date=post.created_date,
                storage_path=f"{post_id}/Graph_DASH.mpd",
                fields={
                    "node": "Came from legacy migration. No migration metadata found."
                },
            )

            static_file_entry = reddit_post_pb2.StaticFileEntry(
                type=reddit_post_pb2.StaticFileType.REDDIT_VIDEO,
                id=video_stream_id,
                path=f"{post_id}/Graph_DASH.mpd",
            )
            post.fields.static_files.append(static_file_entry)

            print(post)
            assert (
                insert_reddit_posts_db(reddit_post=post, secrets=secrets) == 1
            ), logger.error(f"Error in uploading {post.id} migrated post to database")

            assert (
                upload_mpd_reddit_record(
                    reddit_video_content=video_stream_content, secrets=secrets
                )
                == 1
            ), logger.error(
                f"Error in uploading video content from reddit post {video_stream_content.video_stream_id}"
            )

            print(video_stream_content)
