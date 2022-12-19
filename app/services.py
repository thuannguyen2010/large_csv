"""
Service for file processing
"""
import collections
import csv
import shutil
import unittest
from pathlib import Path
from typing import List

import pandas as pd
import pandas.core.frame

from app.utils import get_song_key, get_song_name_and_date_from_song_key


class SongCSVService:
    def __init__(self, request_id, input_file):
        self.input_file = input_file
        self.input_file_name = Path(input_file).stem
        self.directory = f'../outputs/{self.input_file_name}_{request_id}'
        self.tmp_directory = self.directory + '/tmp'
        Path(self.tmp_directory).mkdir(parents=True, exist_ok=True)

    def process(self) -> str:
        """
        processing data from a csv file of songs. Then output is file that contains result
        :return:
        """
        chunk: pandas.core.frame.DataFrame
        for chunk in pd.read_csv(self.input_file, chunksize=1000):
            self.process_songs_data(chunk.values)
        output_file = self.make_result()
        self.clean_up()
        return output_file

    def process_songs_data(self, songs_data: List):
        song_key_to_nums_of_play = collections.defaultdict(int)
        for row in songs_data:
            song_name = row[0]
            date = row[1]
            nums_of_play = row[2]
            song_key = get_song_key(song_name, date)
            song_key_to_nums_of_play[song_key] += nums_of_play

        # update the tmp results
        for song_key, nums_of_play in song_key_to_nums_of_play.items():
            tmp_song_key_file_name = f'{self.tmp_directory}/{song_key}.txt'
            current_nums_of_play = 0
            try:
                with open(tmp_song_key_file_name, mode='rt') as f:
                    data = f.read()
                    if data.isdigit():
                        current_nums_of_play = int(data)
            except IOError:
                pass
            with open(tmp_song_key_file_name, mode='w') as f:
                f.write(str(nums_of_play + current_nums_of_play))

    def make_result(self) -> str:
        output_file = f'{self.directory}/{self.input_file_name}_result.csv'
        with open(output_file, 'w', encoding='utf-8') as csvfile:
            output_writer = csv.writer(csvfile)
            output_writer.writerow(['Song', 'Date', 'Total Number of Plays for Date'])
            files = Path(self.tmp_directory).glob('*')
            rows = []
            chunk_size = 1000
            size = 0
            for file in files:
                file_name = Path(file).stem
                song_name, date = get_song_name_and_date_from_song_key(file_name)
                if not song_name or not date:
                    continue
                with open(file, mode='r') as f:
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
        SongCSVService(1, "../songs.csv").process()
