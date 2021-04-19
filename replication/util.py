import logging
from pathlib import Path

import yaml

from replication.connection import Connection
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


def get_connections(configuration: {}, configuration_name: str) -> [Connection]:
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


# logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
# path = get_path("example.yaml")
# configuration = get_configuration(path)
# connections = get_connections(configuration, 'example')


if __name__ == "__main__":
    connection = Connection(
        host='127.0.0.1',
        port=3306,
        user='root',
        password='root',
        charset='utf8',
        timeout=10,
        server_id=1
    )

    connection_1 = Connection(
        host='127.0.0.1',
        port=3306,
        user='root',
        password='root',
        charset='utf8',
        timeout=10,
        server_id=2
    )

    mysql_service: MySqlService = MySqlService([connection, connection_1], init_schema=False, schema_replica=['public'])
    # print(service.get_table_type_map())
    mysql_service.init()