import pytest
import sqlalchemy as sa
from sqlalchemy import event, text
from geoalchemy2 import load_spatialite
from sqlalchemy.event import listen
from shapely import Polygon

import pandas as pd
import os
import sys

import pysqlite3

sys.modules["sqlite3"] = pysqlite3

from library.types import RedditPostFields, RedditPostDict, PostSpatialLabelDict
from library.io_interfaces.db_io import SQLiteInterface


@pytest.fixture
def sqlite_engine():

    os.environ[
        "SPATIALITE_LIBRARY_PATH"
    ] = "/opt/homebrew/Cellar/libspatialite/5.1.0_1/lib/mod_spatialite.dylib"

    engine = sa.create_engine(
        "sqlite:///:memory:", future=True, plugins=["geoalchemy2"]
    )
    listen(engine, "connect", load_spatialite)

    with engine.connect() as conn, conn.begin():
        conn.execute(
            sa.text(
                """
            CREATE TABLE source (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                created_date TIMESTAMP NOT NULL,
                fields TEXT
            );
        """
            )
        )

        conn.execute(
            sa.text(
                """
            CREATE TABLE labels (
                label_id TEXT PRIMARY KEY,
                post_id TEXT NOT NULL,
                comment TEXT
            );
        """
            )
        )

        conn.execute(
            sa.text(
                """
            SELECT AddGeometryColumn('labels', 'geometry',
                4326, 'POLYGON', 'XY');
        """
            )
        )

        conn.execute(
            sa.text(
                """
            INSERT INTO source (id, type, created_date, fields)
            VALUES
                ('abc123', 'reddit_post', 1723456789, '{"example": "data"}'),
                ('edf456', 'reddit_post', 1723456789, '{"example": "data"}')
            ;
        """
            )
        )

    return engine


@pytest.mark.skip()
def test_add_post_labels(sqlite_engine):

    labels: list[PostSpatialLabelDict] = [
        {
            "post_id": "abc123",
            "label_id": "label001",
            "geometry": "POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))",
            "comment": "This is a test label",
        },
        {
            "post_id": "abc123",
            "label_id": "label002",
            "geometry": "POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))",
            "comment": "This is a test label",
        },
        {
            "post_id": "abc123",
            "label_id": "label003",
            "geometry": "POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))",
            "comment": "This is a test label",
        },
    ]

    inserted_df: pd.DataFrame = SQLiteInterface.add_post_labels(
        labels=labels, db_engine=sqlite_engine, config={}
    )

    print(inserted_df)


@pytest.mark.skip()
def test_get_posts_w_labels(sqlite_engine):

    labels: list[PostSpatialLabelDict] = [
        {
            "post_id": "abc123",
            "label_id": "label001",
            "geometry": "POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))",
            "comment": "This is a test label",
        },
        {
            "post_id": "abc123",
            "label_id": "label002",
            "geometry": "POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))",
            "comment": "This is a test label",
        },
        {
            "post_id": "abc123",
            "label_id": "label003",
            "geometry": "POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))",
            "comment": "This is a test label",
        },
    ]

    SQLiteInterface.add_post_labels(labels=labels, db_engine=sqlite_engine, config={})

    post_w_label = SQLiteInterface.get_post_w_labels(
        id="abc123", db_engine=sqlite_engine, config={}
    )

    print(post_w_label)


@pytest.mark.skip()
def test_get_all_posts_no_label(sqlite_engine):

    posts_wo_label = SQLiteInterface.get_all_unlabeled_posts(
        db_engine=sqlite_engine, config={}
    )
    print(posts_wo_label)


@pytest.mark.skip()
def test_removing_post_labels(sqlite_engine):

    labels: list[PostSpatialLabelDict] = [
        {
            "post_id": "abc123",
            "label_id": "label001",
            "geometry": "POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))",
            "comment": "This is a test label",
        },
        {
            "post_id": "abc123",
            "label_id": "label002",
            "geometry": "POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))",
            "comment": "This is a test label",
        },
        {
            "post_id": "abc123",
            "label_id": "label003",
            "geometry": "POLYGON ((-64.8 32.3, -65.5 18.3, -80.3 25.2, -64.8 32.3))",
            "comment": "This is a test label",
        },
    ]

    SQLiteInterface.add_post_labels(labels=labels, db_engine=sqlite_engine, config={})

    post_w_label = SQLiteInterface.get_post_w_labels(
        id="abc123", db_engine=sqlite_engine, config={}
    )

    print(post_w_label)

    removed_row = SQLiteInterface.remove_post_labels(
        id="label001", db_engine=sqlite_engine, config={}
    )

    print(removed_row)

    post_w_label = SQLiteInterface.get_post_w_labels(
        id="abc123", db_engine=sqlite_engine, config={}
    )

    print(post_w_label)
