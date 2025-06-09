import io
import os
import minio
import traceback
from loguru import logger
from pathlib import Path
from typing import Protocol


class FileInterface(Protocol):
    def upload_file(
        contents_buffer: io.BytesIO, dir_name: str, filepath: str, config: dict
    ) -> str | None:
        """
        Uploads a file to a target directory on the filesystem (S3, local FS, etc...).

        Args:
            contents_buffer (io.BytesIO): The file contents to write.
            dir_name (str): The root directory to write into.
            filepath (str): Relative path from dir_name to write the file to.
            config (dict): Additional config options (unused by local FS).

        Returns:
            str | None: Full path to the uploaded file as a string, or None on failure.
        """
        ...

    def read_file(dir_name: str, filepath: str, config: dict) -> io.BytesIO | str:
        """
        Reads a file from a given directory and path (S3, local FS, etc...).

        Args:
            dir_name (str): The base directory to read from.
            filepath (str): Relative path from dir_name to locate the file.
            config (dict): Additional config options (unused by local FS).

        Returns:
            io.BytesIO | str: A BytesIO stream of file contents on success, or an error message string if the file does not exist.
        """
        ...


class LocalFSInterface(FileInterface):
    def read_file(dir_name: str, filepath: str, config: dict) -> io.BytesIO | None:

        try:
            full_filepath = Path(dir_name) / Path(filepath)
            if not full_filepath.is_file():
                logger.error(f"Cannot read file {full_filepath}. Does not exist")
                return f"Cannot read file {full_filepath}. Does not exist"

            with open(full_filepath, "rb") as f:
                file_stream = io.BytesIO(f.read())
                logger.info(
                    f"Read {file_stream.getbuffer().nbytes} bytes from {full_filepath}"
                )
                return file_stream

        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            return None

    def upload_file(
        contents_buffer: io.BytesIO, dir_name: str, filepath: str, config: dict
    ) -> str | None:
        try:
            logger.info(f"Uploading file to directory {dir_name}")
            full_filepath = Path(dir_name) / Path(filepath)

            os.makedirs(full_filepath.parent, exist_ok=True)

            logger.info(
                f"Writing {contents_buffer.getbuffer().nbytes} bytest directly to {full_filepath}"
            )
            contents_buffer.seek(0)
            with open(full_filepath, "wb") as f:
                written_bytes = f.write(contents_buffer.read())

            logger.info(f"Wrote {written_bytes} to {full_filepath}")

        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            return None

        return str(full_filepath)


class S3FSInterface(FileInterface):
    def read_file(dir_name: str, filepath: str, config: dict) -> io.BytesIO | str:

        MINIO_CLIENT: minio.Minio = config["MINIO_CLIENT"]
        BUCKET_NAME = dir_name

        found = MINIO_CLIENT.bucket_exists(BUCKET_NAME)
        if not found:
            MINIO_CLIENT.make_bucket(BUCKET_NAME)
            logger.info("Created bucket", BUCKET_NAME)
        else:
            logger.info("Bucket", BUCKET_NAME, "already exists")

        try:
            response = MINIO_CLIENT.get_object(BUCKET_NAME, filepath)
            logger.info(
                f"Read {len(response.data)} bytes from bucket {BUCKET_NAME} and filepath {filepath}"
            )
            return io.BytesIO(response.data)

        except Exception as e:
            error_msg = traceback.format_exception(e)
            logger.error(error_msg)
            return error_msg
        finally:
            response.close()
            response.release_conn()
            logger.info("Closed minio connection")

    def upload_file(
        contents_buffer: io.BytesIO, dir_name: str, filepath: str, config: dict
    ) -> str | None:

        MINIO_CLIENT: minio.Minio = config["MINIO_CLIENT"]
        BUCKET_NAME = dir_name

        found = MINIO_CLIENT.bucket_exists(BUCKET_NAME)
        if not found:
            MINIO_CLIENT.make_bucket(BUCKET_NAME)
            logger.info("Created bucket", BUCKET_NAME)
        else:
            logger.info("Bucket", BUCKET_NAME, "already exists")

        try:
            contents_buffer.seek(0)
            logger.info(
                f"Uploading {contents_buffer.getbuffer().nbytes} bytes to bucket {BUCKET_NAME} at path {filepath}"
            )
            result = MINIO_CLIENT.put_object(
                bucket_name=BUCKET_NAME,
                object_name=filepath,
                data=contents_buffer,
                length=contents_buffer.getbuffer().nbytes,
                content_type=config["content_type"]
                if "content_type" in config
                else "application/octet-stream",
            )
            return result.object_name

        except Exception as e:
            error_msg = traceback.format_exception(e)
            logger.error(error_msg)
            return None
