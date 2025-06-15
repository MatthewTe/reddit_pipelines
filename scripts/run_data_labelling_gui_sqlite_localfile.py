import argparse

import sqlalchemy as sa
import sqlalchemy.engine.url as url

from library.io_interfaces.filestore_io import LocalFSInterface
from library.io_interfaces.db_io import SQLiteInterface
from library.ui.data_labeling import generate_data_labelling_dash_app

parser = argparse.ArgumentParser()

parser.add_argument(
    "-f",
    "--file_directory",
    help="The full path for the root path where the static files will be stored",
)

parser.add_argument(
    "-db", "--sqlite_db_path", help="The full filepath to the SQlite database"
)
args = parser.parse_args()

if __name__ == "__main__":

    DB_URI = url.make_url(f"sqlite:////{args.sqlite_db_path}")
    SQLITE_ENGINE: sa.engine.Engine = sa.create_engine(DB_URI)

    sqlite_localfiles_config = {"db_engine": SQLITE_ENGINE}

    app = generate_data_labelling_dash_app(
        db_io=SQLiteInterface, file_io=LocalFSInterface, config=sqlite_localfiles_config
    )
    app.run(debug=True)
