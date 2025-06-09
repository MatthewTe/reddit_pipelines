# Reddit.Old Content Extraction and Spatial Labelling

A colection of APIs and ETL pipelines that are used to extract reddit post content and spatially label them for OSINT purposes. This was developed as a side-project that came out of my desire to organize and validate primary-(ish) sources while studying geopolitical conflicts such as Russia/Ukraine.

## Pipeline usage
The standard workflow for ingesting and classifying data goes like this:

```
1) Write .py script using pipeline API to ingest data from subreddit.
2) Run classification server w/ config pointing it to your pipeline outputs.
3) Use classification GUI to label posts.
4) Export labelled data.
```

### Entrypoint Scripts
The `.py` script that run the ingestion pipeline is the entrypoint to the project. This script is intended to ensure that the data storage backends for the pipeline are set up and that all of the necessary config parameters are passed into the pipeline (the script is responsible for launching the pipeline).

[This](./scripts/run_post_ingestion_postgres_s3.py) is an example of a boilerplate script that runs an ingetion pipeline using the default `FileSystem` and `SQLite` backends. Because these are just scripts that make use of the core API they are not oppinionated about how pipelines are run or how backends are set up. For this example arguments are simply passed via CLI or read as environment variables:

```
python run_post_ingestion_sqlite_localfile.py \
    --env_file=/example_absoloute_path_to_env_file/local.env \
    --reddit_url=https://old.reddit.com/r/UkraineWarVideoReport/  \
    --sqlite_db_path=/example_absoloute_path_to_db_directory/demo_run.sqlite \
    --file_directory=/example_absoloute_path_to_local_directory/
```

### Database Backends
There are two types of backends that the pipeline needs:
- A filestorage backend (S3, Local Filesystem)
- A SQL database backend

The filestorage backend is self-explanatory and the database needs (although this can be overwritten with a new implementation of `DatabaseInterface`) a table called `core` that conforms to the following schema:


| Column Name    | SQL Type      | Description                               |
| -------------- | ------------- | ----------------------------------------- |
| `id`           | `UUID` or `STRING`        | Unique identifier for the post            |
| `type`         | `TEXT`        | Post type (e.g., `'reddit_post'`)         |
| `created_date` | `TIMESTAMPTZ` | UTC timestamp of when the post was added  |
| `fields`       | `JSONB` or `JSON`      | JSON object containing full post metadata |


The `fields` column is a `JSONB` column type that allows for field based searching of the JSON content directly via SQL queries. See [Postgres JSON](https://www.postgresql.org/docs/current/datatype-json.html) Types or [SQLite JSON Data](https://www.sqlite.org/json1.html) for more info.

The core data structure of posts extracted from the pipeline as well as other supporting data-types can be found in the library's [type definition file](./src/library/types.py)

### Pipeline API
```python
import argparser
from selenium import webdriver

from library.io_interfaces.filestore_io import LocalFSInterface
from library.io_interfaces.db_io import SQLiteInterface
from library.reddit_post_extraction_methods import recursive_insert_raw_reddit_post

args = parser.parse_args()

reddit_url: str = args.reddit_url

logger.info(f"Loading secrets from env file {args.env_file}")
dotenv.load_dotenv(args.env_file)

if __name__ == "__main__":

    DB_URI = url.make_url(f"sqlite:////{args.sqlite_db_path}")
    SQLITE_ENGINE: sa.engine.Engine = sa.create_engine(DB_URI)

    with SQLITE_ENGINE.connect() as conn, conn.begin():

        table_create_query = sa.text(
            """
            CREATE TABLE IF NOT EXISTS source (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                created_date TIMESTAMP NOT NULL,
                fields TEXT
            );
            """
        )

        conn.execute(table_create_query)

    sqlite_localfiles_config = {
        "reddit_username": os.environ.get("REDDIT_USERNAME"),
        "reddit_password": os.environ.get("REDDIT_PASSWORD"),
        "db_engine": SQLITE_ENGINE,
        "root_dir_name": args.file_directory,
        "content_type": "",
    }

    driver = webdriver.Chrome()
    driver.implicitly_wait(30)

    inserted_reddit_post_ids: list[str] = []

    logger.info(f"Ingesting Reddit Posts into db:")

    recursive_insert_raw_reddit_post(
        driver=driver,
        page_url=reddit_url,
        config=sqlite_localfiles_config,
        inserted_reddit_ids=inserted_reddit_post_ids,
        file_io=LocalFSInterface,
        database_io=SQLiteInterface,
        login=True,
    )
```

### IO Interfaces
#### TODO: Describe the Interfaces and how to extend them

## Classification Server + GUI
