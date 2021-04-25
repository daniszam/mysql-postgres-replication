import logging
from pathlib import Path

import yaml

from replication.connection import Connection
from replication.error_writer import ErrorWriter
from replication.mysql import MySqlService

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


def get_error_writer(configuration: {}, configuration_name):
    error_file = Path(configuration[configuration_name]['error_file'])
    return ErrorWriter(error_file)


def postgresql_connection(configuration: {}, configuration_name) -> Connection:
    postgresql_conf = configuration[configuration_name]['to']
    postgresql_db = list(postgresql_conf.keys())[0]

    postgresql_conf = postgresql_conf[postgresql_db]
    connection = Connection(
        host=postgresql_conf['host'],
        user=postgresql_conf['user'],
        port=postgresql_conf['port'],
        password=postgresql_conf['password'],
        timeout=postgresql_conf['timeout'],
        charset=postgresql_conf['charset'],
        database=postgresql_conf['database'],
        name=postgresql_db,
    )
    return connection


def mysql_connections(configuration: {}, configuration_name: str) -> [Connection]:
    mysql_configurations = configuration[configuration_name]['from']
    mysql_db_names = mysql_configurations.keys()
    connection_list: [Connection] = []
    for mysql_db_name in mysql_db_names:
        mysql_configuration = mysql_configurations[mysql_db_name]
        connection = Connection(
            host=mysql_configuration['host'],
            user=mysql_configuration['user'],
            port=mysql_configuration['port'],
            password=mysql_configuration['password'],
            timeout=mysql_configuration['timeout'],
            charset=mysql_configuration['charset'],
            name=mysql_db_name,
            server_id=mysql_configuration['server_id']
        )
        connection_list.append(connection)
    return connection_list


if __name__ == "__main__":
    CONF_NAME = 'example'
    logging.basicConfig(filename='example.log', level=logging.DEBUG)
    path = get_path("example.yaml")
    configuration = get_configuration(path)
    connections = mysql_connections(configuration, CONF_NAME)
    postgres_conn = postgresql_connection(configuration, CONF_NAME)
    error_writer = get_error_writer(configuration, CONF_NAME)

    mysql_service: MySqlService = MySqlService(connections, init_schema=False, schema_replica=['public'],
                                               postgres_conf=postgres_conn, error_writer=error_writer)
    mysql_service.init()
