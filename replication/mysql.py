import binascii
import datetime
import logging
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
    GEOMETRY = ['point', 'geometry', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'geometrycollection']

    def __init__(self, connection_conf_list: [Connection],
                 init_schema: bool,
                 schema_replica: [],
                 postgres_conf: Connection,
                 error_writer: ErrorWriter,
                 filter_map: {},
                 batch_size,
                 init_on_start) -> None:
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
        self.special_data_types = self.BLOB + self.GEOMETRY
        self.batch_size = batch_size
        self.init_on_start = init_on_start
        self.postgres_service: PostgreSqlService
        self.mapping = {}
        self.postgres_conf = postgres_conf
        self.error_writer = error_writer
        self.process_list = [Process]
        self.stream_list = [self.init_connection(connection_conf) for connection_conf in connection_conf_list]

    def init(self):
        # create schema if need
        if self.init_schema_on_start and len(self.stream_list) == 1:
            self.init_schema()

        self.start_listen()

    def init_connection(self, connection_conf: Connection) -> (BinLogStreamReader, Connection):
        logging.debug('start init replica')
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
            log_file=None if not self.init_on_start else "binlog.000001",
            log_pos=None if not self.init_on_start else 2792,
            auto_position=None,
            blocking=False,
            resume_stream=self.init_on_start,
            only_schemas=self.schema_replica,
            slave_heartbeat=1,
        )
        return stream, connection_conf

    def start_listen(self):
        for stream, connection_conf in self.stream_list:
            logging.debug('start listen new events')
            self.run(stream, connection_conf)
            # process = Process(target=self.run, args=(stream, connection_conf,))
            # process.start()

    def run(self, stream: BinLogStreamReader, connection_conf: Connection):
        logging.basicConfig(level=logging.DEBUG)
        self.init_postgresql()
        self.batch_insert = []
        while True:
            for binlogevent in stream:
                logging.debug('got a new event')
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
        self.out_dir = "/tmp"
        self.copy_mode = 'file'
        self.copy_max_memory = 300 * 1024 * 1024  # 300mb

    def __init_mysql_replica(self):
        self.init_replica()

    def parse_event(self, binlogevent, connection_conf: Connection):
        table = binlogevent.table
        schema = binlogevent.schema
        logging.debug('event schema:%s table:%s', schema, table)

        event = self.get_event(binlogevent)
        if self.skip_event(table, schema, event) or self.ignore_table(table, schema):
            logging.debug('event will be skipped')
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
                logging.debug('event will be skipped by filter')
                continue

            column_map = table_type_map[schema][table]["column_type"]

            for column in columns:
                type = column_map[column]
                logging.debug('start parsing event')
                if type in self.special_data_types:
                    # decode special types
                    hex_value = binascii.hexlify(columns[column]).decode()
                    columns[column] = binascii.unhexlify(hex_value).decode()
                elif type in self.BLOB and isinstance(old_data[column], bytes):
                    columns[column] = ''
                elif type == 'json':
                    columns[column] = self.parse_dict(old_data[column], connection_conf.charset)
                elif type in self.GEOMETRY and self.postgres_service.check_postgis():
                    columns[column] = self.get_text_geo(old_data[column])

        batch = Batch(
            metadata=metadata,
            old_data=old_data,
            new_data=columns
        )

        self.batch_insert.append(batch)
        logging.debug('add event to a batch %s', batch)

        if len(self.batch_insert) >= self.batch_size:
            logging.debug('send batch to postgres service')
            self.postgres_service.parse_batch(self.batch_insert)
            self.batch_insert.clear()

    def sync_tables(self, cursor_buffered, pg_engine):
        self.__init_sync()
        self.get_table_list(cursor_buffered)
        self.create_destination_schemas()
        try:
            pg_engine.schema_loading = self.schema_replica
            pg_engine.schema_tables = self.get_table_list(cursor_buffered)
            self.create_destination_tables()
            self.disconnect_db_buffered()
            self.__copy_tables()
            pg_engine.grant_select()
            pg_engine.swap_tables()
            self.drop_loading_schemas()
            self.pg_engine.set_source_status("synced")
            self.connect_db_buffered()
            master_end = self.get_master_coordinates()
            self.disconnect_db_buffered()
            self.pg_engine.set_source_highwatermark(master_end, consistent=False)
            self.pg_engine.cleanup_table_events()
            notifier_message = "syncing for tables in schema %s is complete" % self.schema_replica
            logging.info(notifier_message)
        except Exception as e:
            notifier_message = "the move data in source %s failed" % self.schema_replica
            self.error_writer.error(e, notifier_message, datetime.datetime.now(), None)
            raise

    def get_table_list(self, cursor_buffered):
        sql_tables = Query.SELECT_TABLE_BY_SCHEMA
        schema_tables = []
        for schema in self.schema_replica:
            cursor_buffered.execute(sql_tables, (schema))
            table_list = [table["table_name"] for table in cursor_buffered.fetchall()]
            try:
                skip_tables = self.skip_tables[schema]
                if len(skip_tables) > 0:
                    table_list = [table for table in table_list if table not in skip_tables]
            except KeyError:
                pass
            schema_tables[schema] = table_list

        return schema_tables

    def __copy_tables(self, cursor_buffered):
        schema_tables = self.get_table_list(cursor_buffered)
        for schema in self.schema_replica:
            loading_schema = self.schema_replica[schema]["loading"]
            destination_schema = self.schema_replica[schema]["destination"]
            table_list = schema_tables[schema]
            for table in table_list:
                logging.info("Copying the source table %s into %s.%s" % (table, loading_schema, table))
                try:
                    if self.keep_existing_schema:
                        table_pkey = self.pg_engine.get_existing_pkey(destination_schema, table)
                        logging.info("Collecting constraints and indices from the destination table  %s.%s" % (
                            destination_schema, table))
                        self.pg_engine.collect_idx_cons(destination_schema, table)
                        logging.info("Removing constraints and indices from the destination table  %s.%s" % (
                            destination_schema, table))
                        self.pg_engine.cleanup_idx_cons(destination_schema, table)
                        self.pg_engine.truncate_table(destination_schema, table)
                    else:
                        table_pkey = self.__create_indices(schema, table)
                    master_status = self.copy_data(schema, table)
                    self.pg_engine.store_table(destination_schema, table, table_pkey, master_status)
                    if self.keep_existing_schema:
                        self.logger.info("Adding constraint and indices to the destination table  %s.%s" % (
                            destination_schema, table))
                        self.pg_engine.create_idx_cons(destination_schema, table)
                except:
                    logging.info("Could not copy the table %s. Excluding it from the replica." % (table))
                    raise

    def copy_data(self, schema, table, cursor_buffered):
        slice_insert = []
        loading_schema = self.schema_replica[schema]["loading"]

        logging.debug("estimating rows in %s.%s" % (schema, table))
        sql_rows = """
            SELECT
                table_rows as table_rows,
                CASE
                    WHEN avg_row_length>0
                    then
                        round(({}/avg_row_length))
                ELSE
                    0
                END as copy_limit,
                transactions
            FROM
                information_schema.TABLES,
                information_schema.ENGINES
            WHERE
                    table_schema=%s
                AND	table_type='BASE TABLE'
                AND table_name=%s
                AND TABLES.engine = ENGINES.engine
            ;
        """
        sql_rows = sql_rows.format(self.copy_max_memory)
        cursor_buffered.execute(sql_rows, (schema, table))
        count_rows = cursor_buffered.fetchone()
        total_rows = count_rows["table_rows"]
        copy_limit = int(count_rows["copy_limit"])
        table_txs = count_rows["transactions"] == "YES"
        if copy_limit == 0:
            copy_limit = 1000000
        num_slices = int(total_rows // copy_limit)
        range_slices = list(range(num_slices + 1))
        total_slices = len(range_slices)
        slice = range_slices[0]
        logging.debug("The table %s.%s will be copied in %s  estimated slice(s) of %s rows, using a transaction %s" % (
            schema, table, total_slices, copy_limit, table_txs))
        out_file = '%s/%s_%s.csv' % (self.out_dir, schema, table)
        master_status = self.get_master_coordinates()

        select_columns = self.generate_select_statements(schema, table)
        csv_data = ""
        sql_csv = "SELECT * as data FROM `%s`.`%s`;" % (schema, table)
        column_list = select_columns["column_list"]
        logging.debug("Executing query for table %s.%s" % (schema, table))
        if table_txs:
            self.begin_tx()
        self.cursor_unbuffered.execute(sql_csv)
        if table_txs:
            self.unlock_tables()
        while True:
            csv_results = self.cursor_unbuffered.fetchmany(copy_limit)
            if len(csv_results) == 0:
                break
            csv_data = "\n".join(d[0] for d in csv_results)

            if self.copy_mode == 'direct':
                csv_file = io.StringIO()
                csv_file.write(csv_data)
                csv_file.seek(0)

            if self.copy_mode == 'file':
                csv_file = codecs.open(out_file, 'wb', self.charset)
                csv_file.write(csv_data)
                csv_file.close()
                csv_file = open(out_file, 'rb')
            try:
                self.pg_engine.copy_data(csv_file, loading_schema, table, column_list)
            except:
                self.logger.info(
                    "Table %s.%s error in PostgreSQL copy, saving slice number for the fallback to insert statements " % (
                        loading_schema, table))
                slice_insert.append(slice)

            self.print_progress(slice + 1, total_slices, schema, table)
            slice += 1

            csv_file.close()
        if len(slice_insert) > 0:
            ins_arg = {}
            ins_arg["slice_insert"] = slice_insert
            ins_arg["table"] = table
            ins_arg["schema"] = schema
            ins_arg["select_stat"] = select_columns["select_stat"]
            ins_arg["column_list"] = column_list
            ins_arg["copy_limit"] = copy_limit
            self.insert_table_data(ins_arg)

        if table_txs:
            self.end_tx()
        else:
            self.unlock_tables()
        self.cursor_unbuffered.close()
        self.disconnect_db_unbuffered()
        self.disconnect_db_buffered()

        try:
            remove(out_file)
        except:
            pass
        return master_status

    def get_event(self, binlogevent) -> Operation:
        logging.info('parse event type')
        if isinstance(binlogevent, DeleteRowsEvent):
            event = Operation.DELETE
        elif isinstance(binlogevent, UpdateRowsEvent):
            event = Operation.UPDATE
        else:
            event = Operation.INSERT
        return event

    def parse_dict(self, dic_encoded, charset):
        dictionary_decode = {}
        list_decode = []
        if isinstance(dic_encoded, list):
            for item in dic_encoded:
                list_decode.append(self.parse_dict(item))
            return list_decode
        elif not isinstance(dic_encoded, dict):
            try:
                return dic_encoded.decode(charset)
            except Exception:
                return dic_encoded
        else:
            for key, value in dic_encoded.items():
                try:
                    dictionary_decode[key.decode(charset)] = self.parse_dict(value)
                except Exception:
                    dictionary_decode[key] = self.parse_dict(value)
        return dictionary_decode

    def get_text_geo(self, raw_data):
        decoded_data = binascii.hexlify(raw_data)
        return decoded_data.decode()[8:]

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
