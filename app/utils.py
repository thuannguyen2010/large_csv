"""
Utils
"""
import hashlib
import os
from pathlib import Path

DELIMITER = '___'


def get_song_key(song_name: str, date: str):
    return f'{song_name}{DELIMITER}{date}'


def get_song_name_and_date_from_song_key(song_key: str):
    arr = song_key.split(DELIMITER)
    if len(arr) > 1:
        return arr[0], arr[1]
    return None, None


def hash_value(value):
    return int(hashlib.sha1(value.encode("utf-8")).hexdigest()[:5], 16)


def get_number_of_file_in_directory(dir_path):
    count = 0
    for path in os.listdir(dir_path):
        # check if current path is a file
        if os.path.isfile(os.path.join(dir_path, path)):
            count += 1
    return count


def get_project_root() -> Path:
    return Path(__file__).parent.parent
