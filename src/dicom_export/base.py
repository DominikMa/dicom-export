from dataclasses import dataclass

import pypx

import pydantic

class ConnectionSettings():
    server_address: str
    server_port: int = 11112


class Connection:
    def __init__(self, ):
        pass

    def get_series(self):
        find = pypx.Find(settings)
