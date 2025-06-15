import json
import uuid
import sqlalchemy as sa
import pandas as pd
import traceback
import pprint

from loguru import logger
from typing import Protocol
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import ARRAY, UUID, VARCHAR

from library.types import RedditPostDict, PostSpatialLabelDict


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

    def get_all_posts_w_labels(
        db_engine: sa.engine.Engine, config: dict
    ) -> pd.DataFrame | None:
        ...

    def get_all_unlabeled_posts(
        db_engine: sa.engine.Engine, config: dict
    ) -> pd.DataFrame | None:
        ...

    def get_post_w_labels(
        id: str, db_engine: sa.engine.Engine, config: dict
    ) -> dict[RedditPostDict, list[PostSpatialLabelDict]] | None:
        ...

    def replace_post_labels():
        ...

    def add_post_labels(
        labels: list[PostSpatialLabelDict], db_engine: sa.engine.Engine, config: dict
    ) -> pd.DataFrame | None:
        ...

    def remove_post_labels(id: str, db_engine: sa.engine.Engine, config: dict) -> int:
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

    def get_all_posts_w_labels(
        db_engine: sa.engine.Engine, config: dict
    ) -> pd.DataFrame | None:
        with db_engine.connect() as conn, conn.begin():
            posts_w_labels_query = sa.text(
                """"
                SELECT
                    source.id,
                    source.type,
                    source.created_date,
                    source.fields,
                    labels.label_id
                    labels.comment,
                    labels.geometry
                FROM source as source
                JOIN labels as labels
                ON labels.post_id == source.id
            """
            )

            df = pd.read_sql(posts_w_labels_query, con=conn)
        return df

    def add_post_labels(
        labels: list[PostSpatialLabelDict], db_engine: sa.engine.Engine, config: dict
    ) -> pd.DataFrame | None:

        try:
            with db_engine.connect() as conn, conn.begin():

                post_label_insert_query = sa.text(
                    """
                    INSERT INTO labels (label_id, post_id, comment, geometry)
                    VALUES (
                        :label_id,
                        :post_id,
                        :comment,
                        ST_GeomFromText(:geometry, 4326)
                    )
                    """
                )
                for post_label in labels:
                    result = conn.execute(
                        post_label_insert_query, parameters=post_label
                    )
                    logger.info(
                        f"Inserted {result.rowcount} row {pprint.pformat(post_label)} into labels"
                    )

                posts_w_labels_query = sa.text(
                    """
                    SELECT
                        source.id AS post_id,
                        source.type AS post_type,
                        source.created_date AS post_created_date,
                        JSON(source.fields) AS fields,
                        labels.label_id as label_id,
                        labels.comment as label_comment,
                        ST_AsText(labels.geometry) as geometry
                    FROM source as source
                    JOIN labels as labels
                    ON labels.post_id == source.id
                """
                )

                df = pd.read_sql(posts_w_labels_query, con=conn)
                logger.info(df)

            return df

        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            return None

    def get_post_w_labels(
        id: str, db_engine: sa.engine.Engine, config: dict
    ) -> pd.DataFrame | None:

        try:
            with db_engine.connect() as conn, conn.begin():
                posts_w_labels_query = sa.text(
                    """
                    SELECT
                        source.id AS post_id,
                        source.type AS post_type,
                        source.created_date AS post_created_date,
                        JSON(source.fields) AS fields,
                        labels.label_id as label_id,
                        labels.comment as label_comment,
                        ST_AsText(labels.geometry) as geometry
                    FROM source as source
                    JOIN labels as labels
                    ON labels.post_id == source.id
                    WHERE source.id = :id
                """
                )

                df = pd.read_sql(posts_w_labels_query, con=conn, params={"id": id})
                logger.info(df)
                return df

        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            return None

    def get_all_unlabeled_posts(
        db_engine: sa.engine.Engine, config: dict
    ) -> pd.DataFrame | None:
        try:
            with db_engine.connect() as conn, conn.begin():
                posts_w_labels_query = sa.text(
                    """
                    SELECT
                        source.id AS post_id,
                        source.type AS post_type,
                        source.created_date AS post_created_date,
                        JSON(source.fields) AS fields
                    FROM source as source
                    LEFT JOIN labels as labels
                    ON labels.post_id == source.id
                    WHERE labels.post_id IS NULL;
                """
                )

                df = pd.read_sql(posts_w_labels_query, con=conn, params={"id": id})
                logger.info(df)
                return df

        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            return None

    def remove_post_labels(id: str, db_engine: sa.engine.Engine, config: dict) -> int:
        try:
            with db_engine.connect() as conn, conn.begin():

                delete_query = sa.text("DELETE FROM labels WHERE labels.label_id = :id")
                result = conn.execute(delete_query, parameters={"id": id})
                logger.info(
                    f"Deleted row {id} from labels table sucessfully. Rows removed: {result.rowcount}"
                )
                return result.rowcount

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
