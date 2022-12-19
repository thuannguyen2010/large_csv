"""
Utils
"""
import hashlib

DELIMITER = '___'


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def get_song_key(song_name: str, date: str):
    return f'{song_name}{DELIMITER}{date}'


def get_song_name_and_date_from_song_key(song_key: str):
    arr = song_key.split(DELIMITER)
    if len(arr) > 1:
        return arr[0], arr[1]
    return None, None


def hash_value(value):
    return int(hashlib.sha1(value.encode("utf-8")).hexdigest()[:7], 16)


def get_file_url(partition_number):
    path = str(partition_number)
    while partition_number > 1000:
        path = str(partition_number // 1000) + '/' + path
        partition_number = partition_number // 1000
    return path
