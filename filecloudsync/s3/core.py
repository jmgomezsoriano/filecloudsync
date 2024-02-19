import os

import boto3
import hashlib
from typing import Dict, Tuple, Set
from botocore.client import BaseClient
from os import environ, makedirs, utime
from os.path import expanduser, join, exists, dirname

from botocore.exceptions import ClientError
from mysutils.yaml import load_yaml
from mysutils.file import save_json, load_json
from mysutils.hash import file_md5
from dateutil.tz import tzlocal
from logging import getLogger
from enum import Enum

from filecloudsync.file import get_folder_files
from filecloudsync.s3.exceptions import TagsNotMatchError, S3ConnectionError

logger = getLogger(__name__)

ACCESS_KEY_ENV = 'S3_ACCESS_KEY'
SECRET_KEY_ENV = 'S3_SECRET_KEY'
ENDPOINT_ENV = 'S3_ENDPOINT'
S3_CONF_FILE = '.s3conf.yml'
S3_STATUS_FOLDER = '.s3sync'
CONFIG_PATH = config_path = join(expanduser('~'), S3_CONF_FILE)
MISSING_SECRECTS_MSG = (
    'To use this tool you need to set the credentials to access to the S3 bucket.\n'
    'This can be done by the following ways:\n\n'
    f'* Creating the {ACCESS_KEY_ENV}, {SECRET_KEY_ENV} and {ENDPOINT_ENV} environment variables.\n'
    f'* Creating the file "{S3_CONF_FILE}" in your home folder at "{CONFIG_PATH}" with the suitable credentials. '
    f'For example:\n'
    f'aws_access_key_id: <access key>\n'
    f'aws_secret_access_key: <secret key>\n'
    f'endpoint_url: <endpoint URL>\n'
)


class Operation(Enum):
    MODIFIED = "modified"
    DELETED = "deleted"
    ADDED = "added"


class Location(Enum):
    LOCAL = "files"
    BUCKET = "keys"


def key_to_path(folder: str, key: str) -> str:
    return join(folder, *key.split('/'))


def get_credentials(**kwargs) -> dict:
    """

    :param kwargs:
    :return:
    """
    access_key = kwargs.get('aws_access_key_id', environ.get(ACCESS_KEY_ENV, None))
    secret_key = kwargs.get('aws_secret_access_key', environ.get(SECRET_KEY_ENV, None))
    endpoint = kwargs.get('endpoint_url', environ.get(ENDPOINT_ENV, None))
    for file in [S3_CONF_FILE, CONFIG_PATH]:
        if exists(file):
            s3conf = load_yaml(file)
            s3conf['aws_access_key_id'] = s3conf.get('aws_access_key_id', access_key)
            s3conf['aws_secret_access_key'] = s3conf.get('aws_secret_access_key', secret_key)
            s3conf['endpoint_url'] = s3conf.get('endpoint_url', endpoint)
            return s3conf
    return {'aws_access_key_id': access_key, 'aws_secret_access_key': secret_key, 'endpoint_url': endpoint}


def connect(**kwargs) -> BaseClient:
    """

    :param access_key:
    :param secret_key:
    :param endpoint:
    :return:
    """
    try:
        client = boto3.client('s3', **get_credentials(**kwargs))
        client.list_buckets()  # Simple operation to check the connection
        return client
    except ClientError as e:
        raise S3ConnectionError(f'{MISSING_SECRECTS_MSG}\nMore details: {str(e)}')

def upload_file(file: str, client: BaseClient, bucket: str, key: str) -> (float, str):
    client.upload_file(file, bucket, key)
    bucket_object = client.head_object(Bucket=bucket, Key=key)
    last_modified = bucket_object['LastModified'].astimezone(tzlocal()).timestamp()
    utime(file, (last_modified, last_modified))
    return last_modified, bucket_object['ETag'].replace('"', '')


def download_file(client: BaseClient, bucket: str, key: str, dest: str) -> (float, str):
    file_path = destination_path(key, dest)
    makedirs(dirname(file_path), exist_ok=True)
    client.download_file(bucket, key, file_path)
    bucket_object = client.head_object(Bucket=bucket, Key=key)
    last_modified = bucket_object['LastModified'].astimezone(tzlocal()).timestamp()
    utime(file_path, (last_modified, last_modified))
    return last_modified, bucket_object['ETag'].replace('"', '')


def destination_path(key: str, dest: str) -> str:
    return join(dest, *key.split('/'))


def _status_folder() -> str:
    """ Get the configuration folder for all the cache files.

    :return: A folder path.
    """
    return join(expanduser('~'), S3_STATUS_FOLDER)


def status_file(endpoint: str, bucket: str, folder: str, cache_for: Location) -> str:
    """ Get the file path of the synchronization status file.

    :param endpoint: The S3 endpoint
    :param bucket: The bucket name
    :param folder: The folder to synchronize with
    :param cache_for: If the cache is for "files" or for "keys"
    :return: The file path to the status cache file.
    """
    hash_code = hashlib.sha1(''.join([endpoint, bucket, folder]).encode('utf-8')).hexdigest()
    return join(_status_folder(), f'{hash_code}.{cache_for.value}.json.gz')


def _save_sync_status(
        endpoint: str,
        bucket: str,
        folder: str,
        s3_files: Dict[str, Tuple[float, str]],
        local_files: Dict[str, Tuple[float, str]]
) -> None:
    status_folder = _status_folder()
    makedirs(status_folder, exist_ok=True)
    keys = {key: last_modified for key, last_modified in s3_files.items()}
    files = {file: last_modified for file, last_modified in local_files.items()}
    save_json(keys, status_file(endpoint, bucket, folder, Location.BUCKET))
    save_json(files, status_file(endpoint, bucket, folder, Location.LOCAL))


def _load_sync_status_files(
        endpoint: str,
        bucket: str,
        folder: str,
        files: Set[str] = None
) -> (Dict[str, float], Dict[str, float]):
    """ Load the synchronization status file.

    :param endpoint: The S3 endpoint.
    :param bucket: The bucket name.
    :param folder: The folder to synchronize with.
    :param files: A list of keys to watch in Unix file path format.
            If none is given, then check all the bucket/folder files.
    :return: The cached bucket keys and the cached local files with their timestamps and hash.
    """
    keys = _load_sync_status(endpoint, bucket, folder, Location.BUCKET, files)
    files = _load_sync_status(endpoint, bucket, folder, Location.LOCAL, files)
    return keys, files


def load_bucket_sync_status(
        endpoint: str, bucket: str, folder: str, files: Set[str] = None
) -> (Dict[str, float], Dict[str, float]):
    return _load_sync_status(endpoint, bucket, folder, Location.BUCKET, files)


def load_local_sync_status(
        endpoint: str, bucket: str, folder: str, files: Set[str] = None
) -> (Dict[str, float], Dict[str, float]):
    return _load_sync_status(endpoint, bucket, folder, Location.LOCAL, files)


def _load_sync_status(
        endpoint: str,
        bucket: str,
        folder: str,
        where: Location,
        files: Set[str] = None
) -> (Dict[str, float], Dict[str, float]):
    """ Load the synchronization status file.

    :param endpoint: The S3 endpoint
    :param bucket: The bucket name
    :param folder: The folder to synchronize with
    :param where: If the cache is for "files" or for "keys"
    :param files: A list of keys to watch in Unix file path format.
            If none is given, then check all the bucket/folder files
    :return: The cached bucket keys and the cached local files with their timestamps and hash
    """
    status_file_path = status_file(endpoint, bucket, folder, where)
    status = load_json(status_file_path) if exists(status_file_path) else {}
    return {key: value for key, value in status.items() if not files or key in files}


def remove_sync_status(endpoint: str, bucket: str, folder: str) -> None:
    if folder and exists(status_file(endpoint, bucket, folder, Location.BUCKET)):
        os.remove(status_file(endpoint, bucket, folder, Location.BUCKET))
    if folder and exists(status_file(endpoint, bucket, folder, Location.LOCAL)):
        os.remove(status_file(endpoint, bucket, folder, Location.LOCAL))


def _diff_files(
        source_files: Dict[str, Tuple[float, str]],
        cached_files: Dict[str, Tuple[float, str]]
) -> Dict[str, Operation]:
    """ Get the operation changes of a list of files.

    :param source_files: A dictionary with the path to the files and their timestamp and hash.
    :param cached_files: A dictionary with the path to the files and their timestamp and hash.

    :return: A dictionary with the path to the files and the made operations.
    """
    diff = {}

    # Check for added or modified files
    for filename, (source_time, source_etag) in source_files.items():
        cached_time, cached_etag = cached_files.get(filename, (None, None))
        if cached_time is None:
            diff[filename] = Operation.ADDED
        elif source_time != cached_time and source_etag != cached_etag:
            diff[filename] = Operation.MODIFIED

    # Check for deleted files
    for filename in cached_files.keys():
        if filename not in source_files:
            diff[filename] = Operation.DELETED
            source_files[filename] = cached_files[filename]

    return diff


def _sync_download(
        client: BaseClient,
        bucket: str,
        key: str,
        folder: str,
        bucket_files: Dict[str, Tuple[float, str]],
        local_files: Dict[str, Tuple[float, str]]
) -> Tuple[float, str]:
    local_last_modified, local_etag = local_files.get(key, (None, None))
    bucket_last_modified, bucket_etag = bucket_files.get(key, (None, None))
    # If the local bucket does not have the file, download it
    # If the bucket modified time is more recent than the local one, download it
    if key not in local_files or bucket_last_modified and bucket_last_modified > local_last_modified:
        bucket_last_modified, bucket_etag = download_file(client, bucket, key, folder)
        local_last_modified, local_etag = bucket_last_modified, file_md5(key_to_path(folder, key))
        _check_tags(bucket_etag, local_etag)
        bucket_files[key], local_files[key] = (bucket_last_modified, bucket_etag), (bucket_last_modified, bucket_etag)
    # If the local exists and the tags differ, upload it
    elif bucket_etag and bucket_etag != local_etag:
        bucket_last_modified, bucket_etag = upload_file(key_to_path(folder, key), client, bucket, key)
        _check_tags(bucket_etag, local_etag)
        bucket_files[key], local_files[key] = (bucket_last_modified, bucket_etag), (bucket_last_modified, bucket_etag)
    return local_last_modified, local_etag


def _sync_upload(
        client: BaseClient,
        bucket: str,
        key: str,
        folder: str,
        bucket_files: Dict[str, Tuple[float, str]],
        local_files: Dict[str, Tuple[float, str]]
) -> Tuple[float, str]:
    local_last_modified, local_etag = local_files.get(key, (None, None))
    bucket_last_modified, bucket_etag = bucket_files.get(key, (None, None))
    # If the bucket does not have the key, upload it
    # If the local modified time is more recent than the bucket one, upload it
    if key not in bucket_files or local_last_modified and local_last_modified > bucket_last_modified:
        bucket_last_modified, bucket_etag = upload_file(key_to_path(folder, key), client, bucket, key)
        _check_tags(bucket_etag, local_etag)
        bucket_files[key], local_files[key] = (bucket_last_modified, bucket_etag), (bucket_last_modified, bucket_etag)
    # If the bucket exists and the tags differ, download it
    elif local_etag and local_etag != bucket_etag:
        bucket_last_modified, bucket_etag = download_file(client, bucket, key, folder)
        local_last_modified, local_etag = bucket_last_modified, file_md5(key_to_path(folder, key))
        _check_tags(bucket_etag, local_etag)
        bucket_files[key], local_files[key] = (bucket_last_modified, bucket_etag), (bucket_last_modified, bucket_etag)
    return bucket_last_modified, bucket_etag


def _check_tags(bucket_etag, local_etag):
    if bucket_etag != local_etag:
        raise TagsNotMatchError(f'The local tag {local_etag} differs to bucket one {bucket_etag}.')


def _sync_local_remove(
        client: BaseClient,
        bucket: str,
        key: str,
        folder: str,
        bucket_files: Dict[str, Tuple[float, str]],
        local_files: Dict[str, Tuple[float, str]]
) -> None:
    local_last_modified, local_etag = local_files.get(key, (None, None))
    bucket_last_modified, bucket_etag = bucket_files.get(key, (None, None))
    if local_last_modified >= bucket_last_modified or local_etag == bucket_etag:
        client.delete_object(Bucket=bucket, Key=key)
        del local_files[key]
        del bucket_files[key]
    else:
        _sync_upload(client, bucket, key, folder, bucket_files, local_files)


def _sync_bucket_remove(
        client: BaseClient,
        bucket: str,
        key: str,
        folder: str,
        bucket_files: Dict[str, Tuple[float, str]],
        local_files: Dict[str, Tuple[float, str]]
) -> None:
    local_last_modified, local_etag = local_files.get(key, (None, None))
    bucket_last_modified, bucket_etag = bucket_files.get(key, (None, None))
    if bucket_last_modified >= local_last_modified or local_etag == bucket_etag:
        _remove_local_file(folder, key)  # Remove local file like bucket style
        del local_files[key]
        del bucket_files[key]
    else:
        _sync_download(client, bucket, key, folder, bucket_files, local_files)


def _remove_local_file(folder: str, key: str) -> None:
    file = key_to_path(folder, key)
    if os.path.exists(file):
        os.remove(file)
        folder_to_remove = dirname(file)
        # Remove the folder if it is empty
        if not os.listdir(folder_to_remove):
            os.rmdir(folder_to_remove)


def apply_bucket_diffs(
        client: BaseClient,
        bucket: str,
        folder: str,
        bucket_files: Dict[str, Tuple[float, str]],
        local_files: Dict[str, Tuple[float, str]],
        bucket_diff: Dict[str, Operation]
) -> None:
    for key, operation in bucket_diff.items():
        if operation == Operation.ADDED or operation == Operation.MODIFIED:
            _sync_download(client, bucket, key, folder, bucket_files, local_files)
        elif operation == Operation.DELETED:
            _sync_bucket_remove(client, bucket, key, folder, bucket_files, local_files)


def apply_local_diffs(
        client: BaseClient,
        bucket: str,
        folder: str,
        bucket_files: Dict[str, Tuple[float, str]],
        local_files: Dict[str, Tuple[float, str]],
        local_diff: Dict[str, Operation]
) -> None:
    for key, operation in local_diff.items():
        if operation == Operation.ADDED or operation == Operation.MODIFIED:
            _sync_upload(client, bucket, key, folder, bucket_files, local_files)
        elif operation == Operation.DELETED:
            _sync_local_remove(client, bucket, key, folder, bucket_files, local_files)


def sync(client: BaseClient, bucket: str, folder: str, files: Set[str] = None) -> None:
    """ Synchronize a S3 bucket and a folder.

    :param client: The S3 client.
    :param bucket: The bucket name.
    :param folder: The folder to synchronize with.
    :param files: A list of keys to watch in Unix file path format.
            If none is given, then check all the bucket/folder files.
    """
    bucket_diff, bucket_files, local_diff, local_files = check_changes(client, bucket, folder, files)
    try:
        # Apply changes
        apply_bucket_diffs(client, bucket, folder, bucket_files, local_files, bucket_diff)
        apply_local_diffs(client, bucket, folder, bucket_files, local_files, local_diff)
    except TagsNotMatchError as e:
        raise TagsNotMatchError(f'Tags do not match between the bucket "{bucket}" and the folder "{folder}": {str(e)}')
    _save_sync_status(client.meta.endpoint_url, bucket, folder, bucket_files, local_files)


def check_changes(
        client: BaseClient,
        bucket: str,
        folder: str,
        files: Set[str] = None
) -> Tuple[Dict[str, Operation], Dict[str, Tuple[float, str]], Dict[str, Operation], Dict[str, Tuple[float, str]]]:
    """ Check the changes of a S3 bucket and a folder.

    :param client: The S3 client.
    :param bucket: The bucket name.
    :param folder: The folder to check.
    :param files: A list of keys to watch in Unix file path format.
            If none is given, then check all the bucket/folder files.

    :return: A tuple with the bucket differences, the bucket files, the local differences and the local files.
    """
    bucket_diff, bucket_files = check_bucket_changes(client, bucket, folder, files)
    local_diff, local_files = check_local_changes(client, bucket, folder, files)
    return bucket_diff, bucket_files, local_diff, local_files


def check_bucket_changes(
        client: BaseClient,
        bucket: str,
        folder: str,
        files: Set[str] = None
) -> Tuple[Dict[str, Operation], Dict[str, Tuple[float, str]]]:
    """ Check the changes of a S3 bucket with respect to the cached one

    :param client: The S3 client.
    :param bucket: The bucket name.
    :param folder: The folder to check
    :param files: A list of keys to watch in Unix file path format.
            If none is given, then check all the bucket/folder files

    :return: A tuple with the bucket differences and the bucket files
    """
    # Load the bucket cache
    cached_bucket_files = _load_sync_status(client.meta.endpoint_url, bucket, folder, Location.BUCKET, files)
    # List objects in the S3 bucket
    bucket_files = get_bucket_keys(client, bucket, files)
    # Find the differences with respect to the cached
    bucket_diff = _diff_files(bucket_files, cached_bucket_files)
    return bucket_diff, bucket_files


def check_local_changes(
        client: BaseClient,
        bucket: str,
        folder: str,
        files: Set[str] = None
) -> Tuple[Dict[str, Operation], Dict[str, Tuple[float, str]]]:
    """ Check the changes of a S3 bucket with respect to the cached one

    :param client: The S3 client.
    :param bucket: The bucket name.
    :param folder: The folder to check
    :param files: A list of keys to watch in Unix file path format.
            If none is given, then check all the bucket/folder files

    :return: A tuple with the bucket differences and the bucket files
    """
    # Load the bucket cache
    cached_local_files = _load_sync_status(client.meta.endpoint_url, bucket, folder, Location.LOCAL, files)
    # List objects in the S3 bucket
    local_files = get_folder_files(folder, files)
    # Find the differences with respect to the cached
    local_diff = _diff_files(local_files, cached_local_files)
    return local_diff, local_files


def get_bucket_keys(client: BaseClient, bucket: str, files: Set[str] = None) -> Dict[str, Tuple[float, str]]:
    """ Get the timestamp and hash of the bucket keys.

    :param client: The S3 client.
    :param bucket: The bucket name.
    :param files: A list of keys to watch in Unix file path format.
            If none is given, then check all the bucket/folder files.
    :return: A dict with the bucket keys and their timestamp and hash.
    """
    objs = client.list_objects_v2(Bucket=bucket).get('Contents', [])
    return {
        o['Key']: (o['LastModified'].astimezone(tzlocal()).timestamp(), o['ETag'].replace('"', ''))
        for o in objs if not files or o['Key'] in files
    }
