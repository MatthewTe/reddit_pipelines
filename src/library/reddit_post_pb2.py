# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: reddit_post.proto
# Protobuf Python Version: 5.29.3
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    3,
    '',
    'reddit_post.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x11reddit_post.proto\x12\x0breddit_data\"H\n\x0fStaticFileEntry\x12)\n\x04type\x18\x01 \x01(\x0e\x32\x1b.reddit_data.StaticFileType\x12\n\n\x02id\x18\x02 \x01(\t\"9\n\nRedditUser\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x11\n\tfull_name\x18\x03 \x01(\t\"\xf2\x03\n\nRedditPost\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04type\x18\x02 \x01(\t\x12\x14\n\x0c\x63reated_date\x18\x03 \x01(\x02\x12\x38\n\x06\x66ields\x18\x04 \x01(\x0b\x32(.reddit_data.RedditPost.RedditPostFields\x1a\xf9\x02\n\x10RedditPostFields\x12\x11\n\tsubreddit\x18\x01 \x01(\t\x12\x0b\n\x03url\x18\x02 \x01(\t\x12\r\n\x05title\x18\x03 \x01(\t\x12\x1e\n\x16static_downloaded_flag\x18\x04 \x01(\x08\x12\x1c\n\x0fscreenshot_path\x18\x05 \x01(\tH\x00\x88\x01\x01\x12\x1b\n\x0ejson_file_path\x18\x06 \x01(\tH\x01\x88\x01\x01\x12\x19\n\x11post_created_date\x18\x07 \x01(\x02\x12\x1c\n\x0fstatic_root_url\x18\x08 \x01(\tH\x02\x88\x01\x01\x12\x32\n\x0cstatic_files\x18\t \x03(\x0b\x32\x1c.reddit_data.StaticFileEntry\x12*\n\x04user\x18\n \x01(\x0b\x32\x17.reddit_data.RedditUserH\x03\x88\x01\x01\x42\x12\n\x10_screenshot_pathB\x11\n\x0f_json_file_pathB\x12\n\x10_static_root_urlB\x07\n\x05_user\"5\n\x0bRedditPosts\x12&\n\x05posts\x18\x01 \x03(\x0b\x32\x17.reddit_data.RedditPost*W\n\x0eStaticFileType\x12\x10\n\x0cREDDIT_VIDEO\x10\x00\x12\x10\n\x0cREDDIT_IMAGE\x10\x01\x12\x0f\n\x0bREDDIT_TEXT\x10\x02\x12\x10\n\x0cREDDIT_AUDIO\x10\x03\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'reddit_post_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_STATICFILETYPE']._serialized_start=723
  _globals['_STATICFILETYPE']._serialized_end=810
  _globals['_STATICFILEENTRY']._serialized_start=34
  _globals['_STATICFILEENTRY']._serialized_end=106
  _globals['_REDDITUSER']._serialized_start=108
  _globals['_REDDITUSER']._serialized_end=165
  _globals['_REDDITPOST']._serialized_start=168
  _globals['_REDDITPOST']._serialized_end=666
  _globals['_REDDITPOST_REDDITPOSTFIELDS']._serialized_start=289
  _globals['_REDDITPOST_REDDITPOSTFIELDS']._serialized_end=666
  _globals['_REDDITPOSTS']._serialized_start=668
  _globals['_REDDITPOSTS']._serialized_end=721
# @@protoc_insertion_point(module_scope)
