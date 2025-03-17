from neo4j import GraphDatabase
import neo4j
import json
import argparse
import traceback
import typing
import pprint
from datetime import datetime, timezone, time as dt_time
import requests
import io
import sqlalchemy as sa
import time
import random
import os
from minio import Minio
from loguru import logger
import uuid
import xml.etree.ElementTree as ET
from sqlalchemy.dialects.postgresql import ARRAY, UUID

from google.protobuf.message import Message
from google.protobuf.json_format import MessageToJson, MessageToDict


from library.config import Secrets
from library.protobuff_types.reddit import reddit_post_pb2
from library.protobuff_types import core_content_pb2


class RedditVideoInfoDict(typing.TypedDict):
    bitrate_kbps: int
    fallback_url: str
    has_audio: bool
    height: int
    width: int
    scrubber_media_url: str
    dash_url: str
    duration: int
    hls_url: str
    is_gif: bool
    transcoding_status: str


class VideoMPDResult(typing.TypedDict):
    extension: str
    url: str
    mime_type: str
    video_byte_stream: io.BytesIO


class AudioMPDResult(typing.TypedDict):
    extension: str
    url: str
    mime_type: str
    audio_byte_stream: io.BytesIO


class ParsedMPDResult(typing.TypedDict):
    mpd_file: str
    videos_periods: dict[int, VideoMPDResult]
    audio_periods: dict[int, AudioMPDResult]


def parse_video_from_mpd_document(
    reddit_video_info: RedditVideoInfoDict, reddit_post_data: dict
) -> ParsedMPDResult:

    response = requests.get(reddit_video_info["dash_url"])
    logger.info(f"Extracted mpd file from {reddit_video_info['dash_url']}")

    mpd_str: str = response.content.decode()

    root = ET.fromstring(mpd_str)

    namespace = root.tag.split("}")[0].strip("{")

    parsed_result: ParsedMPDResult = {
        "mpd_file": mpd_str,
        "audio_periods": {},
        "videos_periods": {},
    }

    for period in root.findall("Period", namespaces={"": namespace}):
        logger.info(
            f"Period {period.attrib['id']} with a duration of: {period.attrib['duration']}"
        )

        # Getting each Adaptation set for the perod:
        for adaptation_set in period.findall(
            "AdaptationSet", namespaces={"": namespace}
        ):
            logger.info(
                f"Adaptation Set {adaptation_set.attrib['id']} is {adaptation_set.attrib['contentType']} for period {period.attrib['id']}"
            )

            if adaptation_set.attrib["contentType"] == "video":

                # Mapping adaptaion set ids to bandwith values to find the highest resoloution video:
                adaptation_set_video_bandwith_dict = {}

                for representation in adaptation_set.findall(
                    "Representation", namespaces={"": namespace}
                ):
                    adaptation_set_video_bandwith_dict[
                        int(representation.attrib["bandwidth"])
                    ] = int(representation.attrib["id"])

                all_video_bandwidths = [
                    bandwidth for bandwidth in adaptation_set_video_bandwith_dict.keys()
                ]
                all_video_bandwidths.sort()
                largest_video_bandwidth = all_video_bandwidths[0]
                logger.info(
                    f"Found a video for adaptation set {adaptation_set.attrib['id']} that has a bandwidth of {largest_video_bandwidth} in Representation {adaptation_set_video_bandwith_dict[largest_video_bandwidth]}"
                )

                for representation in adaptation_set.findall(
                    "Representation", namespaces={"": namespace}
                ):
                    if (
                        int(representation.attrib["id"])
                        == adaptation_set_video_bandwith_dict[largest_video_bandwidth]
                    ):

                        # TODO: Implement that actual video extraction:
                        logger.info(
                            "Extracted video from largest bandwidth representation set"
                        )

                        reddit_post_base_url = reddit_post_data.get("url", None)
                        video_root_url = representation.find(
                            "BaseURL", namespaces={"": namespace}
                        ).text

                        if (
                            reddit_post_base_url is not None
                            or video_root_url is not None
                        ):
                            video_url = f"{reddit_post_base_url}/{video_root_url}"
                            logger.info(f"Making request to get video from {video_url}")

                            time.sleep(random.randint(1, 3))
                            video_response = requests.get(video_url)
                            video_response.raise_for_status()
                            time.sleep(random.randint(1, 3))

                            video_stream = io.BytesIO(
                                initial_bytes=video_response.content
                            )
                            logger.info(
                                f"Extracted video file from reddit with {len(video_response.content)} bytes"
                            )
                            parsed_result["videos_periods"][
                                int(period.attrib["id"])
                            ] = {
                                "mime_type": representation.attrib["mimeType"],
                                "extension": video_root_url,
                                "url": video_url,
                                "video_byte_stream": video_stream,
                            }

            if adaptation_set.attrib["contentType"] == "audio":

                # Mapping adaptaion set ids to bandwith values to find the highest resoloution video:
                adaptation_set_audio_bandwith_dict = {}

                for representation in adaptation_set.findall(
                    "Representation", namespaces={"": namespace}
                ):
                    adaptation_set_audio_bandwith_dict[
                        int(representation.attrib["bandwidth"])
                    ] = int(representation.attrib["id"])

                all_audio_bandwidths = [
                    bandwidth for bandwidth in adaptation_set_audio_bandwith_dict.keys()
                ]
                all_audio_bandwidths.sort()
                largest_audio_bandwidth = all_audio_bandwidths[0]
                logger.info(
                    f"Found audio for adaptation set {adaptation_set.attrib['id']} that has a bandwidth of {largest_audio_bandwidth} in Representation {adaptation_set_audio_bandwith_dict[largest_audio_bandwidth]}"
                )

                for representation in adaptation_set.findall(
                    "Representation", namespaces={"": namespace}
                ):
                    if (
                        int(representation.attrib["id"])
                        == adaptation_set_audio_bandwith_dict[largest_audio_bandwidth]
                    ):

                        logger.info(
                            "Extracted audio from largest bandwidth representation set"
                        )
                        reddit_post_base_url = reddit_post_data.get("url", None)
                        audio_root_url = representation.find(
                            "BaseURL", namespaces={"": namespace}
                        ).text

                        if (
                            reddit_post_base_url is not None
                            or audio_root_url is not None
                        ):
                            audio_full_url = f"{reddit_post_base_url}/{audio_root_url}"
                            logger.info(
                                f"Making requests to get audio from {audio_full_url}"
                            )

                            time.sleep(random.randint(1, 3))
                            audio_response = requests.get(audio_full_url)
                            audio_response.raise_for_status()
                            time.sleep(random.randint(1, 3))

                            audio_stream = io.BytesIO(
                                initial_bytes=audio_response.content
                            )
                            logger.info(
                                f"Extracted audio file from reddit with {len(audio_response.content)} bytes"
                            )
                            parsed_result["audio_periods"][int(period.attrib["id"])] = {
                                "mime_type": representation.attrib["mimeType"],
                                "extension": audio_root_url,
                                "url": audio_full_url,
                                "audio_byte_stream": audio_stream,
                            }

    return parsed_result


def get_reddit_video_posts(ids: list[str], secrets: Secrets) -> list[dict]:
    psql_engine = sa.create_engine(secrets["psql_uri"])

    with psql_engine.connect() as conn, conn.begin():
        get_all_posts = sa.text(
            """
            SELECT *
            FROM core.source
            WHERE source.id = ANY(:ids)
            AND type='reddit_post'
            AND jsonb_typeof(fields->'staticFiles') = 'array'
            AND EXISTS (
                SELECT 1
                FROM jsonb_array_elements(fields->'staticFiles') AS elem
                WHERE elem->>'type' = 'REDDIT_VIDEO'
                AND (
                    NOT (elem ? 'static_downloaded_flag') -- Key does not exist
                    OR elem->>'static_downloaded_flag' = 'false'
                )
                AND elem->>'id' = 'NULL'
            );
        """
        ).bindparams(sa.bindparam("ids", type_=ARRAY(UUID)))

        results = conn.execute(get_all_posts, {"ids": ids})

    return results.mappings().all()


def update_reddit_post_video_content(
    post_id: str, video_id: str, full_video_path: str, secrets: Secrets
) -> int:

    psql_engine = sa.create_engine(secrets["psql_uri"])

    with psql_engine.connect() as conn, conn.begin():
        update_reddit_post_query = sa.text(
            """
            UPDATE core.source
            SET fields = jsonb_set(
                jsonb_set(
                    fields,
                    '{staticFiles}',
                    (
                        SELECT jsonb_agg(
                            CASE
                                WHEN elem->>'id' = 'NULL' AND elem->>'type' = 'REDDIT_VIDEO'
                                THEN elem || jsonb_build_object('id', :video_id, 'path', :full_video_path)
                                ELSE elem
                            END
                        )
                        FROM jsonb_array_elements(fields->'staticFiles') AS elem
                    )
                ),
                '{static_downloaded_flag}',
                'true'::jsonb,
                true
            )
            WHERE id = :post_id
            """
        )

        update_result = conn.execute(
            update_reddit_post_query,
            {
                "post_id": post_id,
                "video_id": video_id,
                "full_video_path": full_video_path,
            },
        )
        return update_result.rowcount


def upload_mpd_reddit_record(reddit_video_content: Message, secrets: Secrets) -> int:
    psql_engine = sa.create_engine(secrets["psql_uri"])
    with psql_engine.connect() as conn, conn.begin():

        insert_video_stream_query = sa.text(
            """
            INSERT INTO core.content (id, source, type, created_date, storage_path, fields)
            VALUES (:id, :source, :type, :created_date, :storage_path, :fields)
            """
        )
        video_stream_content_to_insert = {
            "id": reddit_video_content.id,
            "source": reddit_video_content.source,
            "type": reddit_video_content.type,
            "created_date": datetime.fromtimestamp(
                reddit_video_content.created_date / 1000, tz=timezone.utc
            ),
            "storage_path": reddit_video_content.storage_path,
            "fields": json.dumps(MessageToDict(reddit_video_content.fields)),
        }

        result = conn.execute(insert_video_stream_query, video_stream_content_to_insert)
        logger.info(f"Inserted {result.rowcount} posts to tables")
        return result.rowcount


def ingest_all_video_data(secrets: Secrets, reddit_ids: list[str] = []):

    all_video_posts: list[dict] = get_reddit_video_posts(reddit_ids, secrets)

    MINIO_CLIENT = Minio(
        secrets["minio_url"],
        access_key=secrets["minio_access_key"],
        secret_key=secrets["minio_secret_key"],
        secure=False,
    )
    BUCKET_NAME = "reddit-posts"

    for video_post in all_video_posts:

        time.sleep(random.randint(2, 4))

        logger.info(
            f"Starting to parse reddit video from node with id {video_post['id']}"
        )

        try:
            response = MINIO_CLIENT.get_object(
                BUCKET_NAME, video_post["fields"]["jsonFilePath"]
            )
            decoded_json = json.loads(response.data)

            response_json = decoded_json[0]["data"]

            post_data = response_json["children"][0]["data"]

            media_dict = post_data.get("secure_media", None)
            if media_dict is not None:
                reddit_video: RedditVideoInfoDict = media_dict.get("reddit_video", None)
                parsed_mpd_result: ParsedMPDResult = parse_video_from_mpd_document(
                    reddit_video, post_data
                )

                logger.info(
                    f"Successfully parsed the mpd result with {len(parsed_mpd_result['videos_periods'].keys())} periods"
                )

                mpd_file_byte_stream = io.BytesIO(
                    parsed_mpd_result["mpd_file"].encode("UTF-8")
                )

                MINIO_CLIENT.put_object(
                    bucket_name=BUCKET_NAME,
                    object_name=f"{video_post['id']}/Origin_DASH.mpd",
                    data=mpd_file_byte_stream,
                    length=mpd_file_byte_stream.getbuffer().nbytes,
                    content_type="application/dash+xml",
                )

                # Creating the new MDP file for the uploaded content:
                mpd_ns = "urn:mpeg:dash:schema:mpd:2011"
                xsi_ns = "http://www.w3.org/2001/XMLSchema-instance"
                ET.register_namespace("", mpd_ns)
                ET.register_namespace("xsi", xsi_ns)
                mpd = ET.Element(
                    "MPD",
                    {
                        "xmlns": mpd_ns,
                        "xmlns:xsi": xsi_ns,
                        "profiles": "urn:mpeg:dash:profile:isoff-on-demand:2011",
                        "type": "static",
                        "xsi:schemaLocation": "urn:mpeg:dash:schema:mpd:2011 DASH-MPD.xsd",
                    },
                )

                # Uploading video files:
                for period_id, video_period in parsed_mpd_result[
                    "videos_periods"
                ].items():

                    period = ET.SubElement(mpd, "Period", {"id": str(period_id)})

                    video_period_filename = (
                        f"{video_post['id']}/{period_id}_{video_period['extension']}"
                    )
                    video_period["video_byte_stream"].seek(0)

                    # Static File Uploads:
                    MINIO_CLIENT.put_object(
                        bucket_name=BUCKET_NAME,
                        object_name=video_period_filename,
                        data=video_period["video_byte_stream"],
                        length=video_period["video_byte_stream"].getbuffer().nbytes,
                        content_type=video_period["mime_type"],
                    )

                    logger.info(
                        f"Uploaded video file to blob at {video_period_filename}"
                    )

                    video_adaptation_set = ET.SubElement(
                        period,
                        "AdaptationSet",
                        {
                            "contentType": "video",
                            "id": str(period_id),
                        },
                    )

                    representation = ET.SubElement(
                        video_adaptation_set,
                        "Representation",
                        {
                            "id": str(period_id),
                            "mimeType": video_period["mime_type"],
                        },
                    )
                    base_url = ET.SubElement(representation, "BaseURL")
                    base_url.text = video_period_filename

                    # Checking to see if video in this period has accompanying audio:
                    audio_period = parsed_mpd_result["audio_periods"].get(
                        period_id, None
                    )
                    if audio_period is not None:
                        logger.info(
                            f"Extracting audio stream for video in period {period_id}"
                        )
                        audio_period_filename = f"{video_post['id']}/{period_id}-{audio_period['extension']}"
                        audio_period["audio_byte_stream"].seek(0)

                        # Static File Uploads:
                        MINIO_CLIENT.put_object(
                            bucket_name=BUCKET_NAME,
                            object_name=audio_period_filename,
                            data=audio_period["audio_byte_stream"],
                            length=audio_period["audio_byte_stream"].getbuffer().nbytes,
                            content_type=audio_period["mime_type"],
                        )

                        logger.info(
                            f"Uploaded audio file to blob at {audio_period_filename}"
                        )

                        audio_adaptation_set = ET.SubElement(
                            period,
                            "AdaptationSet",
                            {
                                "contentType": "audio",
                                "id": str(period_id),
                            },
                        )

                        audio_representation = ET.SubElement(
                            audio_adaptation_set,
                            "Representation",
                            {
                                "id": str(period_id),
                                "mimeType": audio_period["mime_type"],
                            },
                        )
                        base_url = ET.SubElement(audio_representation, "BaseURL")
                        base_url.text = audio_period_filename

                new_mpd_file = ET.tostring(mpd, encoding="unicode", method="xml")
                new_mpd_file_byte_stream = io.BytesIO(new_mpd_file.encode())
                logger.info("Built new MPD file referencing uploaded video files")

                MINIO_CLIENT.put_object(
                    bucket_name=BUCKET_NAME,
                    object_name=f"{video_post['id']}/Video_DASH.mpd",
                    data=new_mpd_file_byte_stream,
                    length=new_mpd_file_byte_stream.getbuffer().nbytes,
                    content_type="application/dash+xml",
                )
                logger.info(f"Uploaded {video_post['id']}/Video_DASH.mpd")

                # Create a content record:
                video_stream_id: str = str(
                    uuid.uuid3(uuid.NAMESPACE_URL, f"{video_post['id']}/Video_DASH.mpd")
                )
                video_stream_path = f"{video_post['id']}/Video_DASH.mpd"
                # Upload an MPD content object:
                utc_datetime = int(
                    datetime.combine(video_post["created_date"], dt_time.min)
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                    * 1000
                )
                video_stream_content = reddit_post_pb2.RedditVideoContent(
                    id=video_stream_id,
                    source=str(video_post["id"]),
                    type=core_content_pb2.CoreContentTypes.VIDEO_DASH_STREAM,
                    created_date=utc_datetime,
                    storage_path=video_stream_path,
                    fields=reddit_video,
                )
                assert upload_mpd_reddit_record(video_stream_content, secrets) == 1

                assert (
                    update_reddit_post_video_content(
                        post_id=video_post["id"],
                        video_id=video_stream_id,
                        full_video_path=video_stream_path,
                        secrets=secrets,
                    )
                    == 1
                )

                logger.info(
                    f"Sucessfully uploaded video stream for reddit post {video_post['id']}"
                )

        except Exception as e:
            logger.error(traceback.format_exc())

        finally:
            response.close()
            response.release_conn()
