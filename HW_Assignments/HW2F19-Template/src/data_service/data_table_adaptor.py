import pymysql
import src.data_service.dbutils as dbutils
import src.data_service.RDBDataTable as RDBDataTable

# The REST application server app.py will be handling multiple requests over a long period of time.
# It is inefficient to create an instance of RDBDataTable for each request.  This is a cache of created
# instances.
_db_tables = {}

def get_rdb_table(table_name, db_name, key_columns=None, connect_info=None):
    """

    :param table_name: Name of the database table.
    :param db_name: Schema/database name.
    :param key_columns: This is a trap. Just use None.
    :param connect_info: You can specify if you have some special connection, but it is
        OK to just use the default connection.
    :return:
    """
    global _db_tables

    # We use the fully qualified table name as the key into the cache, e.g. lahman2019clean.people.
    key = db_name + "." + table_name

    # Have we already created and cache the data table?
    result = _db_tables.get(key, None)

    # We have not yet accessed this table.
    if result is None:

        # Make an RDBDataTable for this database table.
        result = RDBDataTable.RDBDataTable(table_name, db_name, key_columns, connect_info)

        # Add to the cache.
        _db_tables[key] = result

    return result


#########################################
#
#
# YOU HAVE TO IMPLEMENT THE FUNCTIONS BELOW.
#
#
# IMPLEMENTATION
#########################################

def get_databases():
    """

    :return: A list of databases/schema at this endpoint.
    """
    db = 'information_schema'
    table = 'tables'
    rdb_obj = get_rdb_table(table, db)

    q = "select distinct table_schema from " + rdb_obj._full_table_name

    res, data = dbutils.run_q(sql=q, args=None, conn=rdb_obj._cnx, commit=True, fetch=True)

    if data is not None and len(data) > 0:
        data_list = []
        for i in range(len(data)):
            data_list.append(data[i]['TABLE_SCHEMA'])
    else:
        data_list = None

    return data_list

def get_table_names(dbname):
    """
    :return: list of table names in the schema
    """
    db = 'information_schema'
    table = 'tables'
    rdb_obj = get_rdb_table(table, db)

    q = "select distinct table_name from " + rdb_obj._full_table_name
    q += " where table_type = 'BASE TABLE' and table_schema = '" + dbname + "'"

    res, data = dbutils.run_q(sql=q, args=None, conn=rdb_obj._cnx, commit=True, fetch=True)

    if data is not None and len(data) > 0:
        data_list = []
        for i in range(len(data)):
            data_list.append(data[i]['TABLE_NAME'])
    else:
        data_list = None

    return data_list


def get_by_primary_key(table_name, dbname, pk_values, fields=None):
    """
    :return: ouput of the query by primary key values.
    """
    rdb_obj = get_rdb_table(table_name, dbname)
    output = rdb_obj.find_by_primary_key(pk_values, fields)
    # print('db service out:', output)
    return output

def delete_by_primary_key(table_name, dbname, pk_values):
    """
    :return: ouput of the query by primary key values; number of rows deleted.
    """
    rdb_obj = get_rdb_table(table_name, dbname)
    output = rdb_obj.delete_by_key(pk_values)

    return output

def update_by_primary_key(table_name,dbname,pk_values,new_values):
    """
    pk_values: values of primary key fileds provided in an order.
    new_values: dictionary of what the new values should. this is given in the body of api call.
    :return: ouput of the query by primary key values; number of rows updated
    """
    rdb_obj = get_rdb_table(table_name, dbname)
    output = rdb_obj.update_by_key(pk_values, new_values)

    return output

def get_by_template(table_name,dbname,query_param,fields,limit=None,offset=None):
    """
    query_param: template to find records.
    fileds: fields to return
    :return: ouput of the query by primary key values; number of rows updated
    """
    rdb_obj = get_rdb_table(table_name, dbname)
    output = rdb_obj.find_by_template(query_param, fields, limit=limit, offset=offset)

    return output

def insert_record(table_name, dbname, new_record):
    """
    new record: fields to insert
    :return: output of the insert result
    """
    rdb_obj = get_rdb_table(table_name, dbname)
    output = rdb_obj.insert(new_record)

    return output



