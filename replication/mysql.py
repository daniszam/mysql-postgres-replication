import binascii
from multiprocessing import Process

import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from replication.batch import Batch
from replication.connection import Connection
from replication.error_writer import ErrorWriter
from replication.metadata import Metadata
from replication.operation import Operation
from replication.postgresql import PostgreSqlService
from replication.query import Query


class MySqlService(object):
    # special data types
    BLOB = ['blob', 'tinyblob', 'mediumblob', 'longblob', 'binary', 'varbinary']
    SPECIAL = ['point', 'geometry', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'geometrycollection']

    def __init__(self, connection_conf_list: [Connection],
                 init_schema: bool,
                 schema_replica: [],
                 postgres_conf: Connection,
                 error_writer: ErrorWriter,
                 filter_map: {}) -> None:
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
        self.filter_map = filter_map
        self.schema_replica = schema_replica
        self.stream_list = [self.init_connection(connection_conf) for connection_conf in connection_conf_list]
        self.special_data_types = self.BLOB + self.SPECIAL
        self.batch_size = 0
        self.postgres_service: PostgreSqlService
        self.postgres_conf = postgres_conf
        self.error_writer = error_writer
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

    def init_connection(self, connection_conf: Connection) -> (BinLogStreamReader, Connection):
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
            blocking=True,
            resume_stream=True,
            only_schemas="public",
            slave_heartbeat=1,
        )
        return stream, connection_conf

    def start_listen(self):
        for stream, connection_conf in self.stream_list:
            # self.run(stream, connection_conf)
            process = Process(target=self.run, args=(stream, connection_conf,))
            process.start()

    def run(self, stream: BinLogStreamReader, connection_conf: Connection):
        self.init_postgresql()
        while True:
            for binlogevent in stream:
                self.parse_event(binlogevent, connection_conf)

    def init_replica(self):
        self.__init_sync()

    def init_postgresql(self):
        self.postgres_service: PostgreSqlService = PostgreSqlService(connection=self.postgres_conf,
                                                                     error_writer=self.error_writer)
        self.postgres_service.init_connection()

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

    def parse_event(self, binlogevent, connection_conf: Connection):
        table = binlogevent.table
        schema = binlogevent.schema
        batch_insert = []

        event = self.get_event(binlogevent)
        if self.skip_event(table, schema, event) or self.ignore_table(table, schema):
            return

        table_type_map = self.get_table_type_map(connection_conf)

        metadata = Metadata(
            logpos=binlogevent.packet.log_pos,
            schema=schema,
            table=table,
            event_time=binlogevent.timestamp,
            event=event,
        )

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

            table_map = table_type_map[schema]
            for table_name in table_map.keys():
                if table == table_name.lower():
                    table = table_name
                    metadata.table = table_name

            if not self.filter(connection_conf.name, table, schema, columns):
                continue

            column_map = table_type_map[schema][table]["column_type"]

            for column in columns:
                type = column_map[column]

                if type in self.special_data_types:
                    # decode special types
                    columns[column] = binascii.hexlify(columns[column]).decode()

        batch = Batch(
            metadata=metadata,
            old_data=old_data,
            new_data=columns
        )

        batch_insert.append(batch)

        if len(batch_insert) >= self.batch_size:
            self.postgres_service.parse_batch(batch_insert)

    def get_event(self, binlogevent) -> Operation:
        if isinstance(binlogevent, DeleteRowsEvent):
            event = Operation.DELETE
        elif isinstance(binlogevent, UpdateRowsEvent):
            event = Operation.UPDATE
        else:
            event = Operation.INSERT
        return event

    def filter(self, conf_name, table, schema, columns: {}) -> bool:
        try:
            table_filter = self.filter_map[conf_name][schema][table]
        except KeyError:
            return True
        if table_filter is not None:
            for field in columns.keys():
                if field in table_filter:
                    field_filters = table_filter[field]
                    for filter in field_filters.keys():
                        if filter == 'more':
                            if columns[field] <= field_filters['more']:
                                return False
                        if filter == 'less':
                            if columns[field] >= field_filters['less']:
                                return False
                        if filter == 'equal':
                            if columns[field] != field_filters['equal']:
                                return False
                        if filter == 'in':
                            if columns[field] not in field_filters['in']:
                                return False
        return True

    def skip_event(self, table, schema, event) -> bool:
        table_name = "%s.%s" % (schema, table)
        return (schema in self.skip_events[event]) or (table_name in self.skip_events[event])

    def ignore_table(self, table, schema):
        return (schema in self.skip_tables) and (table in self.skip_tables[schema])

    def get_table_type_map(self, connection_conf: Connection):
        connect = pymysql.connect(
            host=connection_conf.host,
            user=connection_conf.user,
            port=connection_conf.port,
            password=connection_conf.password,
            charset=connection_conf.charset,
            connect_timeout=connection_conf.timeout,
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor_buffered = connect.cursor()

        table_type_map = {}
        table_map = {}

        for schema in self.schema_replica:
            sql_tables = Query.SELECT_TABLES

            cursor_buffered.execute(sql_tables, (schema,))
            table_list = cursor_buffered.fetchall()

            for table in table_list:
                column_type = {}
                sql_columns = Query.SELECT_COLUMNS

                table_charset = table["character_set"]
                cursor_buffered.execute(sql_columns, (table["table_schema"], table["table_name"]))
                column_data = cursor_buffered.fetchall()

                for column in column_data:
                    column_type[column["column_name"]] = column["data_type"]

                table_dict = {}
                table_dict["table_charset"] = table_charset
                table_dict["column_type"] = column_type
                table_map[table["table_name"]] = table_dict

            table_type_map[schema] = table_map
            table_map = {}
        return table_type_map
