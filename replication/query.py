class Query(object):
    SELECT_TABLES = "SELECT t.table_schema as table_schema, t.table_name as table_name, SUBSTRING_INDEX(t.TABLE_COLLATION,'_',1) as character_set " \
                    "FROM information_schema.TABLES t " \
                    "WHERE table_type='BASE TABLE' AND	table_schema=%s;"

    SELECT_COLUMNS = " SELECT column_name as column_name,  data_type as data_type " \
                     "FROM information_schema.COLUMNS " \
                     "WHERE  table_schema=%s AND table_name=%s ORDER BY ordinal_position ;"

    POSTGRES_INSERT = "INSERT INTO {}.{} (%s) VALUES (%s);"
    POSTGRES_DELETE = "DELETE FROM {}.{} WHERE %s;"
    POSTGRES_UPDATE = "UPDATE {}.{} SET %s WHERE %s;"

    SELECT_TABLE_BY_SCHEMA = "SELECT table_name as table_name FROM information_schema.TABLES WHERE table_type='BASE TABLE' AND table_schema=%s;"
