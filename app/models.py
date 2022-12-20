from enum import Enum

from dataclasses import dataclass


class Status(Enum):
    PROCESSING = 'processing'
    DONE = 'done'


@dataclass
class Request:
    id: int
    status: str
    file_name: str
