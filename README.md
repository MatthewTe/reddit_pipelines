# Data structures that this pipeline ingests into the database:
protoc --proto_path=/Users/matthewteelucksingh/Repos/psql_schema_exploration/pipelines/reddit --python_out=/Users/matthewteelucksingh/Repos/web_pipelines/reddit_pipelines/src/library /Users/matthewteelucksingh/Repos/psql_schema_exploration/pipelines/reddit/reddit_post.proto

matthewteelucksingh@Matthews-MacBook-Air reddit_pipelines % protoc --proto_path=/Users/matthewteelucksingh/Repos/psql_schema_exploration/pipelines \
       --python_out=/Users/matthewteelucksingh/Repos/web_pipelines/reddit_pipelines/src/library/protobuff_types \
       /Users/matthewteelucksingh/Repos/psql_schema_exploration/pipelines/reddit/reddit_post.proto

source {
    id: string
    type: string default=reddit
    created_date: datetime object
    fields: {
        subreddit: string optional
        url: string
        title: string
        static_downloaded_flag: string
        screenshot_path: string
        json_file_path: string
        post_created_date: datetime
        static_root_url: string
        static_file_type: string
    }
}

then for all of the video and audio, pictures in the reddit post:

{
    id: string
    source: id -> source.id
    type: enum [video, audio, pictures, post?]
    created_date: datetime
    storage_path: string
    fields: {

    }
}
