import datetime
import decimal
import json

import psycopg2
from psycopg2 import sql

from replication.batch import Batch
from replication.connection import Connection
from replication.operation import Operation
from replication.query import Query


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

    def parse_batch(self, batch_list: [Batch]):
        for batch in batch_list:
            metadata = batch.metadata
            if metadata.event == Operation.INSERT:
                self.insert(schema=metadata.schema, table=metadata.table, data=batch.new_data)
            elif metadata.event == Operation.UPDATE:
                self.update()
            elif metadata.event == Operation.DELETE:
                self.delete()

    def insert(self, schema, table, data):
        query = Query.POSTGRES_INSERT % (" ,".join([str(elem) for elem in data.keys()]),
                                         ' ,'.join(["\'" + str(elem) + "\'" for elem in data.values()]))
        query = sql.SQL(query).format(sql.Identifier(schema), sql.Identifier(table))
        self.pgsql_cur.execute(query)

    def delete(self):
        pass

    def update(self):
        pass
