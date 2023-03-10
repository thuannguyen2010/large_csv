import os.path
from pathlib import Path

from app.models import Request
from app.utils import get_number_of_file_in_directory, DELIMITER, get_project_root


class RequestRepository:

    def get_request_by_id(self, request_id):
        try:
            with open(f'{get_project_root()}/db/{request_id}', mode='r') as f:
                data = f.read().strip().split(DELIMITER)
                return Request(id=request_id, file_name=data[0], status=data[1])
        except IOError:
            return None

    def update_request(self, request_id, status):
        request = self.get_request_by_id(request_id)
        with open(f'{get_project_root()}/db/{request_id}', mode='w') as f:
            content = f'{request.file_name}{DELIMITER}{status}'
            f.write(content)
        return request_id

    def create_request(self, file_name) -> int:
        path = os.path.join(get_project_root(), 'db')
        request_id = get_number_of_file_in_directory(path) + 1
        with open(f'{path}/{request_id}', mode='w') as f:
            content = f'{file_name}{DELIMITER}processing'
            f.write(content)
        return request_id
