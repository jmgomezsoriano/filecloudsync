import hashlib

from os import walk
from os.path import join, getmtime, relpath
from typing import Dict, Tuple, Set


def key_to_path(folder: str, key: str) -> str:
    """ Convert a key into a file path

    :param folder: The folder from which the key is synchronized
    :param key: The key to convert
    :return: A path to file in the folder which represents that key
    """
    return join(folder, *key.split('/'))


def file_etag(filename: str, part_size: int = 8 * 1024 * 1024) -> str:
    """ Calculate the S3 eTag from a local file

    :param filename:
    :param part_size: The size of each file part.
        By default, 8MB.
    :return: The eTag
    """
    md5s = []
    with open(filename, 'rb') as f:
        while True:
            data = f.read(part_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))
    if len(md5s) == 1:
        return md5s[0].hexdigest()
    md5 = hashlib.md5()
    for md5_obj in md5s:
        md5.update(md5_obj.digest())
    return f'{md5.hexdigest()}-{len(md5s)}'


def get_folder_files(folder: str, files: Set[str] = None) -> Dict[str, Tuple[float, str]]:
    """ Get the timestamp and content hash of the files in a folder.

    :param folder: The folder where gets the files from
    :param files: A list of keys to watch in Unix file path format.
            If none is given, then it returns all the folder files.
    :return: The file paths relative to folder with their timestamp and hash.
    """
    local_files = {}
    # Walk through the local folder and build the dictionary
    for root, _, folder_files in walk(folder):
        for file in folder_files:
            file_path = join(root, file)
            relative_key = relpath(file_path, folder).replace('\\', '/')
            if not files or relative_key in files:
                local_files[relative_key] = (getmtime(file_path), file_etag(file_path))
    return local_files
