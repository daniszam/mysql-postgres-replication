import binascii
from multiprocessing import Process

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from replication.connection import Connection
from replication.operation import Operation
from replication.query import Query


class MySqlService(object):
    # special data types
    BLOB = ['blob', 'tinyblob', 'mediumblob', 'longblob', 'binary', 'varbinary']
    SPECIAL = ['point', 'geometry', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'geometrycollection']

    def __init__(self, connection_conf_list: [Connection], init_schema: bool) -> None:
        super().__init__()
        self.conn_buffered = None
        self.copy_max_memory = None
        self.skip_events = {
            Operation.UPDATE: [],
            Operation.INSERT: [],
            Operation.DELETE: []
        }
        self.init_schema_on_start = init_schema
        self.skip_tables = {}
        self.schema_replica = []
        self.stream_list = [self.init_connection(connection_conf) for connection_conf in connection_conf_list]
        self.special_data_types = self.BLOB + self.SPECIAL
        self.batch_size = 10
        self.process_list = [Process]

    def init(self):
        # create schema if need
        if self.init_schema_on_start and len(self.stream_list) == 1:
            self.init_schema()

        self.start_listen()

    def init_schema(self):
        """
        The method init schema from mysql db on postgres,
        """
        pass

    def init_connection(self, connection_conf: Connection) -> BinLogStreamReader:
        stream = BinLogStreamReader(
            connection_settings={
                "host": connection_conf.host,
                "port": connection_conf.port,
                "user": connection_conf.user,
                "passwd": connection_conf.password,
                "charset": connection_conf.charset,
                "connect_timeout": connection_conf.timeout
            },
            server_id=connection_conf.server_id,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            log_file="binlog.000001",
            log_pos=2792,
            auto_position=None,
            resume_stream=True,
            only_schemas="public",
            slave_heartbeat=1,
        )
        return stream

    def start_listen(self):
        for stream in self.stream_list:
            process = Process(target=self.run, args=(stream,))
            process.start()

    def run(self, stream: BinLogStreamReader):
        while True:
            for binlogevent in stream:
                binlogevent.dump()

                for row in binlogevent.rows:
                    print(row)

    def init_replica(self):
        self.__init_sync()

    def __init_sync(self):
        """
            The method calls the common steps required to initialise the database connections and
            class attributes within sync_tables,refresh_schema and init_replica.
        """
        # try:
        #     self.source_config = self.sources[self.source]
        # except KeyError:
        #     self.logger.error("The source %s doesn't exists " % (self.source))
        #     sys.exit()
        self.out_dir = "/tmp"
        self.copy_mode = 'file'
        # self.pg_engine.lock_timeout = self.source_config["lock_timeout"]
        # self.pg_engine.grant_select_to = self.source_config["grant_select_to"]
        # if "keep_existing_schema" in self.sources[self.source]:
        #     self.keep_existing_schema = self.sources[self.source]["keep_existing_schema"]
        # else:
        #     self.keep_existing_schema = False
        self.copy_max_memory = 300 * 1024 * 1024  # 300mb
        # if self.postgis_present:
        #     self.hexify = self.hexify_always
        # else:
        #     self.hexify = self.hexify_always + self.spatial_datatypes

        # self.pg_engine.connect_db()
        # self.schema_mappings = self.pg_engine.get_schema_mappings()
        # self.pg_engine.schema_tables = self.schema_tables

    def __init_mysql_replica(self):
        self.init_replica()

    def read_replica_stream(self):

        my_stream = BinLogStreamReader(
            connection_settings={
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "passwd": "root"
            },
            server_id=100,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            log_file="binlog.000001",
            log_pos=2792,
            auto_position=None,
            resume_stream=True,
            only_schemas="public",
            slave_heartbeat=1,
        )

        # while True:

        for binlogevent in my_stream:
            binlogevent.dump()

            for row in binlogevent.rows:
                print(row)

        # my_stream.close()

    def parse_event(self, binlogevent):
        log_position = binlogevent.packet.log_pos
        table = binlogevent.table
        schema = binlogevent.schema
        batch_insert = []

        event = self.get_event(binlogevent)
        if self.skip_event(table, schema, event) or self.ignore_table(table, schema):
            return

        table_type_map = self.get_table_type_map()

        metadata = {
            "binlog": binlogevent.next_binlog,
            "logpos": binlogevent.packet.log_pos,
            "schema": schema,
            "table": table,
            "event_time": binlogevent.timestamp,
            "event": event
        }

        old_data = None
        columns = None
        for row in binlogevent.rows:
            if event == Operation.DELETE:
                columns = row["values"]
            elif event == Operation.UPDATE:
                columns = row["after_values"]
                old_data = row["before_values"]
            elif event == Operation.INSERT:
                columns = row["values"]

            column_map = table_type_map[schema][table]["column_type"]

            for column in columns:
                type = column_map[column]

                if type in self.special_data_types:
                    # decode special types
                    columns[column] = binascii.hexlify(columns[column]).decode()

        parse_event = {
            "metadata": metadata,
            "columns": columns,
            "old_data": old_data
        }

        batch_insert.append(parse_event)

        if (len(batch_insert) >= self.batch_size):
            pass
            # insert batch to postgres

    def get_event(self, binlogevent) -> Operation:
        if isinstance(binlogevent, DeleteRowsEvent):
            event = Operation.DELETE
        elif isinstance(binlogevent, UpdateRowsEvent):
            event = Operation.UPDATE
        else:
            event = Operation.INSERT
        return event

    def skip_event(self, table, schema, event) -> bool:
        """
            Метод проверяет должен ли сработать event

            :param table: Имя таблицы
            :param schema: Схема
            :param event: Event
            :return: true or false
            :rtype: bool
        """
        table_name = "%s.%s" % (schema, table)
        return (schema in self.skip_events[event]) or (table_name in self.skip_events[event])

    def ignore_table(self, table, schema):
        """
        Метод проверяет включена ли таблица в репликацию

        :param table: Имя таблицы
        :param schema: Схема
        :return: true or false
        :rtype: bool
        """
        return (schema in self.skip_tables) and (table in self.skip_tables[schema])

    def get_table_type_map(self):
        """
             Метод создает словарь с ключом для каждой реплицируемой схемы.
             Каждый ключ отображает словарь с таблицами схемы, хранящимися как ключи, и сопоставлениями столбцов / типов.
             Словарь используется в методе read_replica, чтобы определить, требуется ли для поля шестнадцатеричное преобразование.
        """
        table_type_map = {}
        table_map = {}

        for schema in self.schema_replica:
            sql_tables = Query.SELECT_TABLES

            self.cursor_buffered.execute(sql_tables, (schema,))
            table_list = self.cursor_buffered.fetchall()

            for table in table_list:
                column_type = {}
                sql_columns = Query.SELECT_COLUMNS

                table_charset = table["character_set"]
                self.cursor_buffered.execute(sql_columns, (table["table_schema"], table["table_name"]))
                column_data = self.cursor_buffered.fetchall()

                for column in column_data:
                    column_type[column["column_name"]] = column["data_type"]

                table_dict = {}
                table_dict["table_charset"] = table_charset
                table_dict["column_type"] = column_type
                table_map[table["table_name"]] = table_dict

            table_type_map[schema] = table_map
            table_map = {}
        return table_type_map


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

    service: MySqlService = MySqlService([connection, connection_1], init_schema=False)

    service.init()
