
from src.BaseDataTable import BaseDataTable
from src.CaptureException import CaptureException
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()
            if key_columns is not None:
                self._pk_data = {}
                self._load_pk_index()


    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def _load_pk_index(self):
        pk_keys = self._data["key_columns"]
        for i in range(len(self._rows)):
            composite_key = ""
            for val in pk_keys:
                composite_key += self._rows[i][val]
            self._pk_data[composite_key]=self._rows[i]


    def save(self):
        """
        Write the information back to a file.
        :return: None
        """
        name = self._data["connect_info"].get("file_name")
        dir = self._data["connect_info"].get("directory")
        file_path = os.path.join(dir, name)

        with open(file_path, 'w') as f:
            if len(self._rows) > 0:
                writer = csv.DictWriter(f, fieldnames=self._rows[0].keys())
                writer.writeheader()
                writer.writerows(self._rows)
            else:
                f.write('')

    @staticmethod
    def check_headers(headers, fields):

        headers_set = set(headers)
        fields_set = set(fields)

        if fields_set.issubset(headers_set):
            result = True
        else:
            result = False

        return result

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        try:

            pk_keys = self._data["key_columns"]
            template = dict(zip(pk_keys, key_fields))
            result = self.find_by_template(template, field_list)
            return result

        except Exception as err:
            raise err

    def find_by_fast_key(self, composite_key, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param composite_key: A string value with the combination of all key fields.
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        try:

            hdr = list(self._rows[0].keys())

            # if field_list is not None:
            #     check2 = self.check_headers(hdr, field_list)
            #     if not check2:
            #         raise CaptureException("The elements of Field_list do not exist in CSV table")

            result = {}

            result = self._pk_data[composite_key]

            if field_list is not None:
                field_list_vals = []
                for i in field_list:
                    field_list_vals.append(result[i])
                result = dict(zip(field_list, field_list_vals))

            return result

        except Exception as err:
            raise err

    def find_by_primary_key_fast(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        try:

            composite_key = ""
            for val in key_fields:
                composite_key += val

            result = self.find_by_fast_key(composite_key, field_list)
            return result

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

            hdr = list(self._rows[0].keys())

            if template is not None:
                temp_cols = list(template.keys())
                check1 = self.check_headers(hdr, temp_cols)
                if not check1:
                    raise CaptureException("The keys in template do not exist in CSV table")

            if field_list is not None:
                check2 = self.check_headers(hdr, field_list)
                if not check2:
                    raise CaptureException("The elements of Field_list do not exist in CSV table")

            result = []

            for i in range(len(self._rows)):
                current_row = self._rows[i]
                if self.matches_template(current_row, template):
                    if field_list is not None:
                        field_list_vals = []
                        for i in field_list:
                            field_list_vals.append(current_row[i])

                        current_row = dict(zip(field_list, field_list_vals))

                    result.append(current_row)

            return result

        except Exception as err:
            raise err

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        try:

            pk_keys = self._data["key_columns"]
            template = dict(zip(pk_keys, key_fields))
            result = self.delete_by_template(template)
            return result

        except Exception as err:
            raise err

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        try:
            result = self.find_by_template(template)

            count_before_delete = len(self._rows)

            if result:
                temp = []

                for i in range(len(self._rows)):
                    current_row = self._rows[i]
                    if not self.matches_template(current_row, template):
                        temp.append(current_row)
                self._rows = temp

            else:
                print("Delete : No matching records found")

            count_after_delete = len(self._rows)

            rows_deleted = count_before_delete - count_after_delete


            return rows_deleted

        except Exception as err:
            raise err

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        try:

            pk_keys = self._data["key_columns"]
            template = dict(zip(pk_keys, key_fields))
            updated_rows = self.update_by_template(template, new_values)
            return updated_rows

        except Exception as err:
            raise err

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        try:
            rows_for_update = self.find_by_template(template)
            hdr = list(self._rows[0].keys())
            columns_for_update = list(new_values.keys())

            chk_columns = self.check_headers(hdr, columns_for_update)
            if not chk_columns:
                raise CaptureException("The columns you want to update do not exist in CSV Table")

            # This section below identifies if a column in the primary is going to be updated.
            pk_update = False
            for pk_key in self._data["key_columns"]:
                if pk_key in columns_for_update:
                    pk_update = True
                    break

            if rows_for_update:
                updated_rows = []
                for i in range(len(rows_for_update)):
                    current_row = rows_for_update[i]

                    if pk_update:
                        updated_row_pk = []
                        for pk_key in self._data["key_columns"]:
                            if pk_key in columns_for_update:
                                updated_row_pk.append(new_values[pk_key])
                            else:
                                updated_row_pk.append(current_row[pk_key])

                        result = self.find_by_primary_key(updated_row_pk)

                        if result:
                            raise CaptureException("Primary key already exists with the updated values ")

                    # This is modification of data in update statement
                    for j in columns_for_update:
                        current_row[j] = new_values[j]

                    updated_rows.append(current_row)

                return len(updated_rows)

            else:
                print("No matching records found")

        except Exception as err:
            raise err

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        try:

            fields = list(new_record.keys())
            hdr = list(self._rows[0].keys())

            # Check if the column names provided in the insert statement exists in the CSV table
            cols_exists = all(cols in hdr for cols in fields)

            # If all columns exists, check if the primary key is already present in the table
            if cols_exists:
                # Obtain primary key values of the table

                pk_values = []
                pk_keys = self._data["key_columns"]

                for pk_cols in pk_keys:
                    pk_values.append(new_record[pk_cols])

                pk_template = dict(zip(pk_keys, pk_values))

                result = self.find_by_template(pk_template)

                if result:
                    raise CaptureException("Primary key already exists ")
                else:
                    self._add_row(new_record)

            else:
                raise CaptureException("The column names in the new record are not in CSV Table ")

        except Exception as err:
            raise err

    def get_rows(self):
        return self._rows

