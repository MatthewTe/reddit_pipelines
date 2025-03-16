from loguru import logger
import pandas as pd 
from datetime import datetime
import uuid


def extract_author_from_json(comment_data: dict) -> dict:
    try:
        author = comment_data['author']
    except Exception as e:
        logger.warning(f"Unable to extract author from post json so setting author to none")
        author = "not_found"

    if author == '[deleted]':
        associated_user = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, "deleted_user")),
            "author_full_name": "deleted_user",
            "author_name": author
        }
    elif author == 'not_found':
        associated_user = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, "not_found")),
            "author_full_name": "not_found",
            "author_name": author
        }
    else:
        associated_user = {
            "id": str(uuid.uuid3(uuid.NAMESPACE_URL, comment_data['author_fullname'])),
            "author_full_name": comment_data['author_fullname'],
            "author_name": author
        }

    return associated_user


def recursively_build_comment_creation_lst(output_lst: list[dict], post, comment_json_obj, parent_object: dict = None):

    comment_listing_type = comment_json_obj["kind"]

    if comment_listing_type == "t1":
        comment_data = comment_json_obj["data"]
        associated_author = extract_author_from_json(comment_data)

        comment_full_id: str = comment_data["id"]
        comment_node_datetime_obj = pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s")
        comment_node = {
            "type":"node",
            "query_type":"MERGE",
            "labels":["Reddit", "Entity", "Comment"],
            "properties": {
                "id": comment_full_id,
                "body": comment_data["body"],
                "datetime": comment_node_datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        }

        post_day_str: str = comment_node_datetime_obj.strftime("%Y-%m-%d").replace('"', '')
        day_date_id = str(uuid.uuid3(uuid.NAMESPACE_URL, post_day_str))
        day_node = {
            "type":"node",
            "query_type":"MERGE",
            "labels":["Date"],
            "properties": {
                "id": day_date_id,
                "day": post_day_str
            }
        }

        author_node = {
            "type":"node",
            "query_type":"MERGE",
            "labels":["Reddit", "User", "Entity", "Account"],
            "properties": {
                "id": associated_author["id"],
                "author_name": associated_author["author_name"],
                "author_full_name": associated_author["author_full_name"],
            }
        }

        author_post_comment_edge = {
            "type":"edge",
            "labels": ["POSTED"],
            "connection": {
                "from": associated_author["id"],
                "to": comment_full_id
            },
            "properties": {
                "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        }

        comment_to_day_edge = {
            "type":"edge",
            "labels": ["POSTED_ON"],
            "connection": {
                "from": comment_full_id,
                "to": day_date_id
            },
            "properties": {}
        }

        comment_to_post_edge = {
            "type":"edge",
            "labels":["COMMENTED_ON"],
            "connection": {
                "from": comment_full_id,
                "to": post['id']
            },
            "properties": {
                "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        }
        output_lst.append(comment_node)
        output_lst.append(author_node)
        output_lst.append(day_node)
        output_lst.append(author_post_comment_edge)
        output_lst.append(comment_to_day_edge)
        output_lst.append(comment_to_post_edge)

        # If this is a recursive call that has provided a parent comment node (it is a reply) connect the current comment to the parent comment:
        if parent_object is not None:
            reply_to_parent_comment_edge = {
                "type":"edge",
                "query_type":"MERGE",
                "labels":["REPLIED_TO"],
                "connection": {
                    "from": comment_full_id,
                    "to": parent_object["properties"]["id"]
                },
                "properties": {
                    "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            }
            parent_to_reply_comment_edge = {
                "type":"edge",
                "query_type":"MERGE",
                "labels":["HAS_REPLY"],
                "connection": {
                    "from": parent_object["properties"]["id"],
                    "to": comment_full_id
                },
                "properties": {
                    "datetime": pd.to_datetime(int(comment_data['created_utc']), utc=True, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            }
            output_lst.append(reply_to_parent_comment_edge)
            output_lst.append(parent_to_reply_comment_edge)

        replies = comment_data.get("replies", None)
        if isinstance(replies, dict):
            if replies['kind'] != "more":
                for reply in replies["data"]['children']:
                    recursively_build_comment_creation_lst(output_lst, post, reply, parent_object=comment_node)
        else:
            return

    elif comment_listing_type == "more":
        logger.warning("Recursive post extraction has hit the 'more' component. Exiting recursive extraction.")
        return
