import psycopg2
from psycopg2 import sql

from replication.batch import Batch
from replication.connection import Connection
from replication.operation import Operation
from replication.query import Query


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
                self.update(schema=metadata.schema, table=metadata.table, old_data=batch.old_data,
                            new_data=batch.new_data)
            elif metadata.event == Operation.DELETE:
                print(batch)
                self.delete(schema=metadata.schema, table=metadata.table, data=batch.new_data)

    # TODO add batch exception handling
    def insert(self, schema, table, data):
        query = Query.POSTGRES_INSERT % (" ,".join([str(elem) for elem in data.keys()]),
                                         ' ,'.join(["\'" + str(elem) + "\'" for elem in data.values()]))
        query = sql.SQL(query).format(sql.Identifier(schema), sql.Identifier(table))
        self.pgsql_cur.execute(query)

    def delete(self, schema, table, data):
        where_statement = ' and '.join("{}=%s".format(key) for key in data.keys())
        query = Query.POSTGRES_DELETE % where_statement
        query = sql.SQL(query).format(sql.Identifier(schema), sql.Identifier(table))
        self.pgsql_cur.execute(query, list(data.values()))

    def update(self, schema, table, old_data, new_data):
        set_statement = ', '.join("{}=%s".format(key) for key in new_data.keys())
        where_statement = ' and '.join("{}=%s".format(key) for key in old_data.keys())
        query = Query.POSTGRES_UPDATE % (set_statement, where_statement)
        query = sql.SQL(query).format(sql.Identifier(schema), sql.Identifier(table))
        self.pgsql_cur.execute(query, list(new_data.values()) + list(old_data.values()))
