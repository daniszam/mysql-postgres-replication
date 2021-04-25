import os
from datetime import datetime
from pathlib import Path
import json as json_writer


class ErrorWriter(object):

    def __init__(self, log_path: Path) -> None:
        super().__init__()
        self.path = log_path

    def error(self, e: BaseException, message: str, date: datetime, data):
        json = self.__build__json(e, message, date, data, 'ERROR')
        self.__write__(json)

    def severe(self, e: BaseException, message: str, date: datetime, data):
        json = self.__build__json(e, message, date, data, 'SEVERE')
        self.__write__(json)

    def debug(self, e: BaseException, message: str, date: datetime, data):
        json = self.__build__json(e, message, date, data, 'DEBUG')
        self.__write__(json)

    def info(self, e: BaseException, message: str, date: datetime, data):
        json = self.__build__json(e, message, date, data, 'INFO')
        self.__write__(json)

    def __build__json(self, e: BaseException, message: str, date: datetime, data, level: str) -> {}:
        json = {
            'level': level,
            'message': message,
            'date': date,
            'data': data,
            'exception': e
        }
        return json

    def __write__(self, json: {}):
        write_data = [json]
        if self.path.exists():
            with self.path.open(mode='r') as outfile:
                data = json_writer.load(outfile)
                data.append(json)
                write_data = data

        with self.path.open(mode='w') as outfile:
            json_writer.dump(write_data, outfile, default=str)
