"""
Test
"""
import collections
import csv
import os
import shutil
import unittest
from pathlib import Path

from fastapi.testclient import TestClient
from fastapi import BackgroundTasks

from app.main import app, process_request
from app.models import Status
from app.utils import get_project_root, DELIMITER


class TestCSVHandler(unittest.TestCase):

    def setUp(self) -> None:
        self.test_file = 'test.csv'
        self.sample_data = [
            ["Song", "Date", "Number of Plays"],
            ["Umbrella", "2020-01-02", "200"],
            ["Umbrella", "2020-01-01", "100"],
            ["In The End", "2020-01-01", "500"],
            ["Umbrella", "2020-01-01", "50"],
            ["In The End", "2020-01-01", "1000"],
            ["Umbrella", "2020-01-02", "50"],
            ["In The End", "2020-01-02", "500"]
        ]
        with open(self.test_file, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(self.sample_data)

    def tearDown(self):
        os.remove(self.test_file)
        shutil.rmtree(str(get_project_root()) + "/outputs")

    def test_upload_file_then_return_success(self):
        client = TestClient(app)
        with open(self.test_file, mode="rb") as f:
            response = client.post("/upload", files={"file": ("test.csv", f, "text/csv")})
            code = response.json()['code']
            assert code == 'success'

    def test_process_csv_file_then_return_success(self):
        get_project_root()
        with open(f'{get_project_root()}/inputs/{self.test_file}', 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(self.sample_data)
        request_id = 1
        with open(f'{get_project_root()}/db/{request_id}', 'w') as f:
            f.write(f'{self.test_file}{DELIMITER}{Status.PROCESSING.value}')
        background_tasks = BackgroundTasks()
        process_request(request_id, self.test_file, background_tasks)
        with open(f'{get_project_root()}/outputs/{Path(self.test_file).stem}_{request_id}/result_{self.test_file}',
                  'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            data = collections.defaultdict(dict)
            for row in csv_reader:
                data[row[0]][row[1]] = row[2]
            self.assertEqual(data["Umbrella"]["2020-01-01"], "150")
            self.assertEqual(data["Umbrella"]["2020-01-02"], "250")
            self.assertEqual(data["In The End"]["2020-01-01"], "1500")
            self.assertEqual(data["In The End"]["2020-01-02"], "500")
