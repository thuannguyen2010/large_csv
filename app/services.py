"""
Service for file processing
"""
import collections
import csv
import os
import shutil
import time
import tracemalloc
import unittest
from pathlib import Path, PurePath
from typing import List

import pandas as pd
import pandas.core.frame

from app.utils import get_song_key, get_song_name_and_date_from_song_key, hash_value


def linecount_wc(input_file):
    return int(os.popen(f'wc -l {input_file}').read().split(' ')[0])


class SongCSVService:
    def __init__(self, request_id, input_file):
        self.input_file = input_file
        self.input_file_name = Path(input_file).stem
        self.directory = f'../outputs/{self.input_file_name}_{request_id}'
        self.tmp_directory = self.directory + '/tmp'
        Path(self.tmp_directory).mkdir(parents=True, exist_ok=True)

    # @profile
    def process(self) -> str:
        """
        processing data from a csv file of songs. Then output is file that contains result
        :return:
        """
        start = time.time()
        count = linecount_wc(self.input_file)
        print(count)
        n = len(str(count // 1000))
        print(n)
        chunk: pandas.core.frame.DataFrame
        for chunk in pd.read_csv(self.input_file, chunksize=1000):
            self.process_songs_data(chunk.values, n)
        print(f'Process took {int(time.time() - start)} seconds')
        output_file = self.make_result()
        print(f'Make result took {int(time.time() - start)} seconds')
        self.clean_up()
        print(f'Function took {int(time.time() - start)} seconds')
        return output_file

    def get_file_url(self, partition_number, file_name):
        path = str(partition_number)
        while partition_number > 1000:
            path = str(partition_number // 1000) + '/' + path
            partition_number = partition_number // 1000
        path = self.tmp_directory + '/' + path + '/' + file_name
        return path

    def process_songs_data(self, songs_data: List, n: int):
        song_key_to_nums_of_play = collections.defaultdict(int)
        for row in songs_data:
            song_name = row[0]
            date = row[1]
            nums_of_play = row[2]
            song_key = get_song_key(song_name, date)
            song_key_to_nums_of_play[song_key] += nums_of_play

        # update the tmp results
        for song_key, nums_of_play in song_key_to_nums_of_play.items():
            partition_number = hash_value(song_key)
            tmp_song_key_file_name = self.get_file_url(partition_number, song_key+'.txt')
            current_nums_of_play = 0
            try:
                os.makedirs(os.path.dirname(tmp_song_key_file_name), exist_ok=True)
                with open(tmp_song_key_file_name, mode='rt') as f:
                    data = f.read()
                    if data.isdigit():
                        current_nums_of_play = int(data)
            except IOError:
                pass
            with open(tmp_song_key_file_name, mode='w') as f:
                f.write(str(nums_of_play + current_nums_of_play))

    # @profile
    def make_result(self) -> str:
        output_file = f'{self.directory}/{self.input_file_name}_result.csv'
        with open(output_file, 'w', encoding='utf-8') as csvfile:
            output_writer = csv.writer(csvfile)
            output_writer.writerow(['Song', 'Date', 'Total Number of Plays for Date'])
            rows = []
            chunk_size = 1000
            size = 0
            for path, subdirs, files in os.walk(self.tmp_directory):
                for file in files:
                    p = PurePath(path, file)
                    file_name = p.stem
                    song_name, date = get_song_name_and_date_from_song_key(file_name)
                    if not song_name or not date:
                        continue
                    with open(p, mode='r') as f:
                        data = f.read()
                        if not data.isdigit():
                            continue
                        nums_of_play = int(data)
                    rows.append([song_name, date, nums_of_play])
                    size += 1
                    if size >= chunk_size:
                        output_writer.writerows(rows)
                        size = 0
                        rows = []
        return output_file

    def clean_up(self):
        shutil.rmtree(self.tmp_directory)


class TestABC(unittest.TestCase):

    def test_song_csv_processing(self):
        tracemalloc.start()
        SongCSVService(1, "../songs_1_Mi.csv").process()
        current, peak = tracemalloc.get_traced_memory()
        print(current / 1024 / 1024, peak / 1024 / 1024)