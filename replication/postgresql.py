import datetime
import decimal
import io
import json

import psycopg2
from psycopg2 import sql

from replication.connection import Connection


class pg_encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.time) or \
                isinstance(obj, datetime.datetime) or \
                isinstance(obj, datetime.date) or \
                isinstance(obj, decimal.Decimal) or \
                isinstance(obj, datetime.timedelta) or \
                isinstance(obj, set) or \
                isinstance(obj, frozenset) or \
                isinstance(obj, bytes):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class PostgreSqlService(object):

    def __init__(self, connection: Connection) -> None:
        super().__init__()
        self.connection = connection
        self.pgsql_conn = None
        self.pgsql_cur = None

    def init_connection(self):
        if self.connection and not self.pgsql_conn:
            strconn = "dbname=%(database)s user=%(user)s host=%(host)s password=%(password)s port=%(port)s" \
                      % {
                          "database": self.connection.database,
                          "user": self.connection.user,
                          "host": self.connection.host,
                          "password": self.connection.password,
                          "port": self.connection.port
                      }
            self.pgsql_conn = psycopg2.connect(strconn)
            self.pgsql_conn.set_client_encoding(self.connection.charset)
            self.pgsql_conn.set_session(autocommit=True)
            self.pgsql_cur = self.pgsql_conn.cursor()

    def write_batch(self, group_insert):
        csv_file = io.StringIO()
        insert_list = []
        for row_data in group_insert:
            global_data = row_data["metadata"]
            event_after = row_data["columns"]
            event_before = row_data["old_data"]
            # log_table = global_data["log_table"]
            insert_list.append(self.pgsql_cur.mogrify("%s,%s,%s,%s,%s,%s,%s", (
                global_data["table"],
                global_data["schema"],
                str(global_data["event"]),
                global_data["logpos"],
                json.dumps(event_after, cls=pg_encoder),
                json.dumps(event_before, cls=pg_encoder),
                global_data["event_time"])))

        csv_data = b"\n".join(insert_list).decode()
        csv_file.write(csv_data)
        csv_file.seek(0)
        try:
            sql_copy = sql.SQL("""
                COPY "sch_chameleon".{}
                    (
                        v_table_name,
                        v_schema_name,
                        enm_binlog_event,
                        i_binlog_position,
                        jsb_event_after,
                        jsb_event_before,
                        i_my_event_time
                    )
                FROM
                    STDIN
                    WITH NULL 'NULL'
                    CSV QUOTE ''''
                    DELIMITER ','
                    ESCAPE ''''
                ;
            """).format(sql.Identifier("t_log_replica_mysql_1"))
            self.pgsql_cur.copy_expert(sql_copy, csv_file)
        except psycopg2.Error as e:
            print(e)

    def insert(self, schema, table, data, column_list):
        pass

    def delete(self):
        pass

    def update(self):
        pass
