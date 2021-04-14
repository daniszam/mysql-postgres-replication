import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from replication.connection import Connection
from replication.operation import Operation
from replication.query import Query


class MySqlService(object):

    def __init__(self, connection_conf) -> None:
        super().__init__()
        self.conn_buffered = None
        self.copy_max_memory = None
        self.skip_events = {
            Operation.UPDATE: [],
            Operation.INSERT: [],
            Operation.DELETE: []
        }
        self.skip_tables = {}
        self.schema_replica = []
        self.connection_conf = connection_conf

    def connection(self, connection_conf: Connection):
        self.conn_buffered = pymysql.connect(
            host=connection_conf.host,
            user=connection_conf.user,
            port=connection_conf.port,
            password=connection_conf.password,
            charset=connection_conf.charset,
            connect_timeout=connection_conf.timeout,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.charset = db_conn["charset"]
        self.cursor_buffered = self.conn_buffered.cursor()
        self.cursor_buffered_fallback = self.conn_buffered.cursor()

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

        self.connection()
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

        while True:

            for binlogevent in my_stream:
                binlogevent.dump()

                for row in binlogevent.rows:
                    print(row)

        my_stream.close()

    def parse_event(self, binlogevent):
        log_position = binlogevent.packet.log_pos
        table = binlogevent.table
        schema = binlogevent.schema

        if self.skip_event(table, schema, binlogevent) or self.ignore_table(table, schema):
            return

        for row in binlogevent.rows:
            column_map = table_type_map[schema_row][table_name]["column_type"]

    def skip_event(self, table, schema, binlogevent) -> bool:
        """
            Метод проверяет должен ли сработать event

            :param table: Имя таблицы
            :param schema: Схема
            :param binlogevent: Event
            :return: true or false
            :rtype: bool
        """
        if isinstance(binlogevent, DeleteRowsEvent):
            event = Operation.DELETE
        elif isinstance(binlogevent, UpdateRowsEvent):
            event = Operation.UPDATE
        else:
            event = Operation.INSERT

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
        charset='UTF-8',
        timeout=10
    )

    service: MySqlService = MySqlService(connection)

    service.read_replica_stream()
