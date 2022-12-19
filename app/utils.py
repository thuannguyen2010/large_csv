"""
Utils
"""

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
