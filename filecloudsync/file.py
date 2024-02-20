from os import walk
from os.path import join, getmtime, relpath
from typing import Dict, Tuple, Set

from mysutils.hash import file_md5


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
                local_files[relative_key] = (getmtime(file_path), file_md5(file_path))
    return local_files
