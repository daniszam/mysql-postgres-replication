from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, GtidEvent, HeartbeatLogEvent
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent


class MySqlService(object):

    def __init__(self) -> None:
        super().__init__()
        self.conn_buffered = None
        self.charset = None
        self.cursor_buffered = {}
        self.cursor_buffered_fallback = {}
        self.copy_max_memory = None

    def connection(self):
        pass
        # self.conn_buffered = pymysql.connect(
        #     host=db_conn["host"],
        #     user=db_conn["user"],
        #     port=db_conn["port"],
        #     password=db_conn["password"],
        #     charset=db_conn["charset"],
        #     connect_timeout=db_conn["connect_timeout"],
        #     cursorclass=pymysql.cursors.DictCursor
        # )
        # self.charset = db_conn["charset"]
        # self.cursor_buffered = self.conn_buffered.cursor()
        # self.cursor_buffered_fallback = self.conn_buffered.cursor()

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
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent, GtidEvent,
                         HeartbeatLogEvent],
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
                if isinstance(binlogevent, GtidEvent):
                    break
                elif isinstance(binlogevent, HeartbeatLogEvent):
                    break
                elif isinstance(binlogevent, RotateEvent):
                    break
                elif isinstance(binlogevent, QueryEvent):
                    break
                else:
                    for row in binlogevent.rows:
                        print(row)

        my_stream.close()


if __name__ == "__main__":
    service: MySqlService = MySqlService()
    service.read_replica_stream()
