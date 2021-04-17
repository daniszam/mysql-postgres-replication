import logging
from pathlib import Path

import yaml

from replication.connection import Connection

type_dictionary = {
    'integer': 'integer',
    'mediumint': 'bigint',
    'tinyint': 'integer',
    'smallint': 'integer',
    'int': 'integer',
    'bigint': 'bigint',
    'varchar': 'character varying',
    'character varying': 'character varying',
    'text': 'text',
    'char': 'character',
    'datetime': 'timestamp without time zone',
    'date': 'date',
    'time': 'time without time zone',
    'timestamp': 'timestamp without time zone',
    'tinytext': 'text',
    'mediumtext': 'text',
    'longtext': 'text',
    'tinyblob': 'bytea',
    'mediumblob': 'bytea',
    'longblob': 'bytea',
    'blob': 'bytea',
    'binary': 'bytea',
    'varbinary': 'bytea',
    'decimal': 'numeric',
    'double': 'double precision',
    'double precision': 'double precision',
    'float': 'double precision',
    'bit': 'integer',
    'year': 'integer',
    'enum': 'enum',
    'set': 'text',
    'json': 'json',
    'bool': 'boolean',
    'boolean': 'boolean',
}


def get_path(str_path: str):
    try:
        return Path(str_path)
    except NotImplementedError:
        logging.error("Could not be resolved path=%s", str_path)


def get_configuration(path: Path) -> {}:
    with path.open() as configuration_file:
        configuration = yaml.load(configuration_file, Loader=yaml.FullLoader)
        logging.debug("read yaml file=%s", configuration)
        return configuration


def get_connection(configuration: {}) -> Connection:
    mysql_configuration = configuration['example']['from']['mysql_1']
    connection = Connection(
        host=mysql_configuration['host'],
        user=mysql_configuration['user'],
        port=mysql_configuration['port'],
        password=mysql_configuration['password'],
        timeout=mysql_configuration['timeout'],
        charset=mysql_configuration['charset']
    )
    return connection


logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
path = get_path("example.yaml")
configuration = get_configuration(path)
connection = get_connection(configuration)
print(connection)