from typing import TypedDict, Optional


class RedditUserDict(TypedDict):
    id: str
    author_name: str
    author_full_name: str


class RedditPostFields(TypedDict):
    subreddit: str
    url: str
    title: str
    static_downloaded_flag: bool
    screenshot_path: Optional[str]
    json_file_path: Optional[str]
    post_created_date: float
    static_root_url: Optional[str]
    static_files: list[dict]
    user: Optional[RedditUserDict]


class RedditPostDict(TypedDict):
    id: str
    type: str
    created_date: float
    fields: RedditPostFields


# core.labels  post_id | label_id | extent_geometry | label_comment


class PostSpatialLabelDict(TypedDict):
    post_id: str
    label_id: str
    geometry: str
    comment: str
