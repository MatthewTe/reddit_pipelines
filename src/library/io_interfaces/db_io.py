import json
import uuid
import sqlalchemy as sa
import traceback
import pprint

from loguru import logger
from typing import Protocol
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import ARRAY, UUID, VARCHAR

from library.types import RedditPostDict


class DatabaseInterface(Protocol):
    def get_unique_posts(
        ids: list[str], db_engine: sa.engine.Engine, config: dict
    ) -> list[str] | None:
        """
        Retrieve the IDs of posts that already exist in the database.

        Args:
            ids (list[str]): A list of Reddit post IDs to check.
            db_engine (sa.engine.Engine): SQLAlchemy engine.
            config (dict): Additional config data dict.

        Returns:
            list[str] | None: List of post IDs that exist, or None on error.
        """
        ...

    def insert_reddit_posts_db(
        reddit_post: dict, db_engine: sa.engine.Engine, config: dict
    ) -> str | None:
        """
        Insert a single Reddit post into the database.

        Args:
            reddit_post (RedditPostDict): A dictionary containing Reddit post data.
            db_engine (sa.engine.Engine): SQLAlchemy engine for database connection.
            config (dict): Configuration dictionary (currently unused).

        Returns:
            Optional[str]: A log message describing the insert result, or None if an error occurs.
        """
        ...

    def upload_mpd_reddit_record():
        ...

    def update_reddit_post_video_content():
        ...


class SQLiteInterface(DatabaseInterface):
    def get_unique_posts(
        ids: list[str], db_engine: sa.engine.Engine, config: dict
    ) -> list[str] | None:

        try:
            with db_engine.connect() as conn, conn.begin():
                get_unique_id_query = sa.text(
                    """
                SELECT id
                FROM source
                WHERE source.id IN :ids
                AND type = 'reddit_post'
                """
                ).bindparams(sa.bindparam("ids", expanding=True))

                logger.info(f"Querying all ids that already exists from id set {ids}")
                existing_id_results = conn.execute(get_unique_id_query, {"ids": ids})
                existing_ids_dicts = existing_id_results.mappings().all()

            return [str(post["id"]) for post in existing_ids_dicts]

        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            return None

    def insert_reddit_posts_db(
        reddit_post: RedditPostDict, db_engine: sa.engine.Engine, config: dict
    ) -> str | None:

        try:
            with db_engine.connect() as conn, conn.begin():
                insert_query = sa.text(
                    """
                    INSERT INTO source (id, type, created_date, fields)
                    VALUES (:id, :type, :created_date, :fields);
                    """
                )

                posts_to_insert = {
                    "id": reddit_post["id"],
                    "type": reddit_post["type"],
                    "created_date": datetime.fromtimestamp(
                        reddit_post["created_date"] / 1000, tz=timezone.utc
                    ),
                    "fields": json.dumps(reddit_post["fields"]),
                }

                result = conn.execute(insert_query, posts_to_insert)
                result_message = f"Inserted {result.rowcount} posts to tables {pprint.pformat(posts_to_insert)}"
                logger.info(result_message)

                return result_message

        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            return None


class PostgresInterface(DatabaseInterface):
    def get_unique_posts(
        ids: list[str], db_engine: sa.engine.Engine, config: dict
    ) -> list[str] | None:
        try:
            with db_engine.connect() as conn, conn.begin():
                get_unique_id_query = sa.text(
                    """
                SELECT id
                FROM core.source
                WHERE source.id = ANY(:ids)
                AND type = 'reddit_post'
                """
                ).bindparams(sa.bindparam("ids", type_=ARRAY(UUID)))

                logger.info(f"Querying all ids that already exists from id set {ids}")
                existing_id_results = conn.execute(
                    get_unique_id_query, {"ids": [uuid.UUID(id) for id in ids]}
                )
                existing_ids_dicts = existing_id_results.mappings().all()

            return [str(post["id"]) for post in existing_ids_dicts]

        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            return None

    def insert_reddit_posts_db(
        reddit_post: RedditPostDict, db_engine: sa.engine.Engine, config: dict
    ) -> str | None:

        try:
            with db_engine.connect() as conn, conn.begin():
                insert_query = sa.text(
                    """
                    INSERT INTO core.source (id, type, created_date, fields)
                    VALUES (:id, :type, :created_date, :fields);
                    """
                )

                posts_to_insert = {
                    "id": reddit_post["id"],
                    "type": reddit_post["type"],
                    "created_date": datetime.fromtimestamp(
                        reddit_post["created_date"] / 1000, tz=timezone.utc
                    ),
                    "fields": json.dumps(reddit_post["fields"]),
                }

                result = conn.execute(insert_query, posts_to_insert)
                result_message = f"Inserted {result.rowcount} posts to tables {pprint.pformat(posts_to_insert)}"
                logger.info(result_message)

                return result_message

        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            return None
