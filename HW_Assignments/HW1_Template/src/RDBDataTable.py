from src.BaseDataTable import BaseDataTable
from src.CaptureException import CaptureException
import pymysql
import json
import pandas as pd
import logging


class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if table_name is None or connect_info is None:
            raise ValueError("Invalid input.")

        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }

        cnx = self._get_connection(connect_info)

        if cnx is not None:
            self._cnx = cnx
        else:
            raise CaptureException("Could not get a connection.")

    def __str__(self):

        result = "RDBDataTable:"
        result += json.dumps(self._data['table_name'], indent=2)
        result += json.dumps(self._data['key_columns'], indent=2)

        row_count = self._get_row_count()
        result += "\nNumber of rows = " + str(row_count)

        some_rows = pd.read_sql(
            "select * from " + self._data["table_name"] + " limit 10",
            con=self._cnx
        )
        result += "\nFirst 10 rows = \n"
        result += str(some_rows)

        return result

    def _get_row_count(self):

        sql = "select count(*) as count from " + self._data["table_name"]
        res, d = self._run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
        row_count = d[0]['count']

        return row_count

    def _get_connection(self, connect_info):
        """
        :param connect_info: A dictionary containing the information necessary to make a PyMySQL connection.
        :return: The connection. May raise an Exception/Error.
        """
        connect_info['cursorclass']=pymysql.cursors.DictCursor
        cnx = pymysql.connect(**connect_info)
        return cnx

    def _run_q(self, sql, args=None, fetch=True, cur=None, conn=None, commit=True):
        '''
        Helper function to run a SQL statement.
        An RDBDataTable MUST have a connection specified by the connection information.
        This means that this implementation of run_q MUST NOT try to obtain
        a default connection.

        :param sql: SQL template with placeholders for parameters. Cannot be NULL.
        :param args: Values to pass with statement. May be null.
        :param fetch: Execute a fetch and return data if TRUE.
        :param conn: The database connection to use. This cannot be NULL, unless a cursor is passed.
        :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
        :param commit: This is wizard stuff.

        :return: A pair of the form (execute response, fetched data). There will only be fetched data if
            the fetch parameter is True. 'execute response' is the return from the connection.execute, which
            is typically the number of rows effected.
        '''

        cursor_created = False
        connection_created = False
        logger = logging.getLogger()

        try:

            if conn is None:
                raise ValueError("In this implementation, conn cannot be None.")

            if cur is None:
                cursor_created = True
                cur = conn.cursor()

            if args is not None:
                log_message = cur.mogrify(sql, args)
            else:
                log_message = sql

            logger.debug("Executing SQL = " + log_message)

            res = cur.execute(sql, args)

            if fetch:
                data = cur.fetchall()
            else:
                data = None

            # Do not ask.
            if commit == True:
                conn.commit()

        except Exception as e:
            raise (e)

        return (res, data)

    def _convert_template_for_query(self, template):
        """
        Convert a query template into a WHERE clause.
        :param t: Query template.
        :return: (WHERE clause, arg values for %s in clause)
        """
        keys = []
        values = []
        where_clause = ""

        # The where clause will be of the for col1=%s, col2=%s, ...
        # Build a list containing the individual col1=%s
        # The args passed to +run_q will be the values in the template in the same order.
        for k, v in template.items():
            temp = k + "=%s "
            keys.append(temp)
            values.append(v)

        if len(keys) > 0:
            where_clause = "WHERE " + " AND ".join(keys)
        else:
            where_clause = ""
            values = None

        return where_clause, values

    def _insert_func(self, table_name, column_list, values_list, cnx=None, commit=True):
        """
        :param table_name: Name of the table to insert data. Probably should just get from the object data.
        :param column_list: List of columns for insert.
        :param values_list: List of column values.
        :param cnx: Should be always passed
        :param commit: Ignore this for now.
        :return:
        """

        q = "insert into " + table_name + " "

        # If the column list is not None, form the (col1, col2, ...) part of the statement.
        if column_list is not None:
            q += "(" + ",".join(column_list) + ") "

        # We will use query parameters. For a term of the form values(%s, %s, ...) with one slot for
        # each value to insert.
        values = ["%s"] * len(values_list)

        # Form the values(%s, %s, ...) part of the statement.
        values = " ( " + ",".join(values) + ") "
        values = "values" + values

        # Put all together.
        q += values

        res,data = self._run_q(q, args=values_list, conn=cnx, fetch=False)

        return data

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        try:

            pk_keys = self._data["key_columns"]

            pk_template = dict(zip(pk_keys, key_fields))

            w_clause, args = self._convert_template_for_query(pk_template)

            if field_list is None:
                field_list = ['*']

            q = "SELECT " + ",".join(field_list) + " FROM " + self._data['table_name'] + " " + w_clause

            res, data = self._run_q(q, args=args, conn=self._cnx, fetch=True)
            return data

        except Exception as err:
            raise err

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        try:
            w_clause, args = self._convert_template_for_query(template)

            if field_list is None:
                field_list = ['*']
            q = "SELECT " + ",".join(field_list) + " FROM " + self._data["table_name"] + " " + w_clause
            res, data = self._run_q(q, args=args, conn=self._cnx, fetch=True)

            return data

        except Exception as err:
            raise err

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        try:
            pk_keys = self._data['key_columns']
            pk_template = dict(zip(pk_keys, key_fields))

            w_clause, args = self._convert_template_for_query(pk_template)

            q = "DELETE FROM " + self._data['table_name'] + " " + w_clause

            res, data = self._run_q(q, args=args, conn=self._cnx, fetch=False)
            return res

        except Exception as err:
            raise err

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        try:
            w_clause, args = self._convert_template_for_query(template)

            q = "DELETE FROM " + self._data['table_name'] + " " + w_clause

            res, data = self._run_q(q, args=args, conn=self._cnx, fetch=False)
            return res

        except Exception as err:
            raise err

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        try:
            pk_keys = self._data['key_columns']
            pk_template = dict(zip(pk_keys, key_fields))

            w_clause, args = self._convert_template_for_query(pk_template)

            update_clause = " set "

            for (key, val) in new_values.items():
                update_clause += key + "='" + val + "',"

            update_clause = update_clause[:-1]
            q = "UPDATE " + self._data['table_name'] + update_clause + " " + w_clause

            res, data = self._run_q(q, args=args, conn=self._cnx, fetch=False)
            return res

        except Exception as err:
            raise err

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        try:
            w_clause, args = self._convert_template_for_query(template)

            update_clause = " set "

            for (key, val) in new_values.items():
                update_clause += key + "='" + val + "',"

            update_clause = update_clause[:-1]
            q = "UPDATE " + self._data['table_name'] + update_clause + " " + w_clause

            res, data = self._run_q(q, args=args, conn=self._cnx, fetch=False)
            return res

        except Exception as err:
            raise err

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        try:
            new_keys = list(new_record.keys())
            new_values = list(new_record.values())

            data = self._insert_func(self._data['table_name'], new_keys, new_values, self._cnx)

        except Exception as err:
            raise (err)

    def get_rows(self):
        sql = "select * from " + self._data["table_name"]
        res, data = self._run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
        self._rows = data
        return self._rows




