import csv
import src.CSVCatalog as CSVCatalog #File you updated
import json
import os
import tabulate #prints tables nicely

'''
Instruction: 
find_by_template() determines if there is an applicable index (access path) that can be 
used to accelerate a find_by_template(). If there is an applicable index, find_by_template 
used the index by calling __find_by_template_index__(). 

execute_join() performs a join of the table (self) with the input table. 
The method includes a list of the on_columns. The column names are the same in both tables. 
There is also a where_template and field_list to apply to the result of the execute_join(). 
Based on reviewing the lecture material, you should easily be able to identify two vastly different, 
significant optimizations. Your solution should document the optimizations in comments and implement 
the optimizations.
'''
class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog()

    def __init__(self, t_name, load=True):
        """
        Constructor.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name

        # Holds loaded metadata from the catalog. You have to implement the called methods below.
        self.__description__ = None

        if load:
            self.__load_info__()
            # Load metadata

            self.__rows__ = []

            # Build indexes defined in the metadata, we can build indexes on load.
            self.__load__()

            #added this
            self.file_name = self.__description__.file_name

        else:
            self.file_name = "DERIVED"


    def __get_file_name__(self):
        t = self.__description__
        return t.file_name

    def __add_row__(self, row):
        """
        adds a row to the table definition from the csv file
        also creates the appropriate index based on col vals in row
        :param row: delineated by the CSV itself
        :return:
        """

        self.__rows__.append(row)
        defined_indexes = self.__description__.indexes

        for index in defined_indexes:
            name = index.index_name
            key_string = self.__get_key__(index, row)

            if key_string in self.__keys_added__:
                self.__indexes__[name][key_string].append(row)
            else:
                self.__indexes__[name][key_string] = []
                self.__indexes__[name][key_string].append(row)
                self.__keys_added__.append(key_string)
        return

    def __get_key__(self, index, row):
        """
        this function returns a dictionary such as:
        {'David_Aardsma': {'bats': 'R', 'bbrefID': 'aardsda01', 'birthCity': 'Denver', 'birthCou...

        :param index:
        :param row:
        :return:
        """
        key = []

        cl = CSVCatalog.CSVCatalog().get_table(self.__table_name__).get_index_cols(index.index_name)

        for name in cl:
            key.append(row[name])

        kstring = "_".join(key)
        return kstring



    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.
        :return:
        """
        self.__description__ = CSVCatalog.CSVCatalog().get_table(self.__table_name__)
        return

    # Load from a file and creates the table and data.
    def __load__(self):

        self.__indexes__ = {}

        given_indexes = self.__description__.indexes
        self.__keys_added__ = []

        for index in given_indexes:
            self.__indexes__[index.index_name] = {}

        try:
            _data_dir = (os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'data/')).replace('\\', '/')
            _metadata_dir = (os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'database/')).replace('\\','/')
            fn = _data_dir + self.__get_file_name__()
            print("Loading "+self.__get_file_name__()+"....")

            with open(fn, "r") as csvfile:
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Get the names of the columns defined for this table from the metadata.
                column_names = self.__get_column_names__()

                # Loop through each line (well dictionary) in the input file.
                for r in reader:
                    # Only add the defined columns into the in-memory table. The CSV file may contain columns
                    # that are not relevant to the definition.
                    projected_r = self.project([r], column_names)[0]
                    self.__add_row__(projected_r)
            print("Done loading")

        except IOError as e:
            raise ValueError("Could not read file = " + fn)

    def __get_column_names__(self):
        column_names = []
        column_list = self.__description__.columns
        for c in column_list:
            column_names.append(c.column_name)
        return column_names

    def __get_row_count__(self):
        if self.__rows__ is not None:
            return len(self.__rows__)
        else:
            return 0

    def __str__(self):
        """
        :return:
        """

        data = self.__rows__
        header = data[0].keys()
        rows = [x.values() for x in data]
        return tabulate.tabulate(rows, header, tablefmt='grid')


    def get_access_path(self, fields):
        """
        Returns best index matching the set of keys in the template.

        Best is defined as the most selective index, i.e. the one with the most distinct index entries.

        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the index name. The template may have additional columns, but must contain
        all of the columns in the index definition.
        :param tmp: Query template.
        :return: Index or None
        """
        if self.__indexes__ == {}: #loaded table, empty does not mean none
            return None, None
        else:
            result = None
            count = None

        if fields is None:
            return None

        tmp_set = set(fields)

        # examine each of the indexes
        description = self.__description__


        idx_list = self.__description__.indexes

        if idx_list is None:
            return None, None


        actual_index_list = []
        for i in idx_list:
            columns = set(i.column_names)
            if columns.issubset(tmp_set):

                if result is None: #means we havent found an applicable index
                    result = i.index_name
                    count = len(self.__indexes__[result])
                else:
                    if count < len(self.__indexes__[i.index_name]):
                        #this is a selectivity check to make sure our index is most efficient
                        result = i.index_name
                        count = len(self.__indexes__[result])
        return result, count


    def matches_template(self, row, t):
        """
        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.
        """

        # Basically, this means there is no where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True
        except Exception as e:
            raise (e)

    def project(self, rows, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None:  # If there is not project clause, return the base table
                return rows  # Should really return a new, identical table but am lazy.
            else:
                result = []
                for r in rows:  # For every row in the table.
                    tmp = {}  # Not sure why I am using range.
                    for j in range(0, len(fields)):  # Make a new row with just the requested columns/fields.
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)  # Insert into new table when done.

                return result

        except KeyError as ke:
            # happens if the requested field not in rows.
            raise ValueError("Invalid field in project")

    def __find_by_template_scan__(self, t, fields=None, limit=None, offset=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all row if template is None and all columns if fields is None.
        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :param limit: Max to return. Not implemented
        :param offset: Offset into the result. Not implemented.
        :return: New table containing the result of the select and project.
        """

        if limit is not None or offset is not None:
            raise ValueError("Limit/offset not supported for CSVTable")

        # If there are rows and the template is not None
        if self.__rows__ is not None:
            result = []

            # Add the rows that match the template to the newly created table.
            for r in self.__rows__:
                if self.matches_template(r, t):
                    result.append(r)
            result = self.project(result, fields)
        else:
            result = None

        return result

    def __find_by_template_index__(self, t, idx, fields=None, limit=None, offset=None):

        """
        Find using a selected index
        :param t: Template representing a where clause/
        :param idx: Name of index to use.
        :param fields: Fields to return.
        :param limit: Not implemented. Ignore.
        :param offset: Not implemented. Ignore
        :return: Matching tuples.
        """
        #does the index match the template?


        if limit is not None or offset is not None:
            raise ValueError("Limit/offset not supported for CSVTable")

        result = []

        # gets the dictionary of the index name given
        # {"TeamID": {"BOS":[list of dicitonary rows with BOS]}, {"CL1":[list of dictionaryrows with CL1]}}
        indexed_rows = self.__indexes__[idx]

        # given the template, makes the appropriate key to look for
        # i.e. if the template is {teamID:BOS, yearID: 1924} but index is only on teamID it will return the key_value "BOS"
        index_def = self.__description__.get_index(idx)
        key_value = self.__get_key__(index_def, t)
        #get_key, given this template and this index
        #won't check if key is actually viable

        #it takes the index object, not the index name, so pull the index by it's name to get the object


        try:
            #if you try to give it a dictionary that doesnt have a value it will throw an exception
            # gets the list of dictionary rows matching the key value
            keyed_rows = indexed_rows[key_value]

        except:
            return None


        for row in keyed_rows:
            if self.matches_template(row, t):
                result.append(row)
        final_result = self.project(result, fields)
        return final_result

    #find_by_template happens after the where pushdown
    #when you do the pushdown, you get a new table (a derived table that is not indexed)
    #take the On clause and the Where clause and use that for the find

    def find_by_template(self, template, fields=None, limit=None, offset=None):
        #get the most selective, if it doesnt exist, otherwise

        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.

        if self.file_name == "DERIVED":  # No indexes will be applied to derived tables
            result_rows = self.__find_by_template_scan__(template, fields, limit, offset)

        elif self.__indexes__ is None:  # also should mean a derived table or some other issue
            result_rows = self.__find_by_template_scan__(template, fields, limit, offset)

        else:
            result_index, count = self.get_access_path(template)
            if result_index is not None:
                result_rows = self.__find_by_template_index__(template, result_index, fields, limit, offset)
            else:
                result_rows = self.__find_by_template_scan__(template, fields, limit, offset)

        return result_rows




  # self, right_r, on_fields, where_template, project_fields)
    def dumb_join(self, right_r, on_fields, where_template=None, project_fields=None, optimize = True):
        """

        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """

        result = []
        for sr in self.__rows__:
            join_template = right_r.__get_on_template__(sr, on_fields)
            for pr in right_r.__rows__:
                if right_r.matches_template(pr,join_template):
                    joined_row = {**sr, **pr}
                    result.append(joined_row)

        print('Rows in left table ', self.__table_name__, ': ', len(self.__rows__))
        print('Rows in right table ', right_r.__table_name__, ': ', len(right_r.__rows__))
        print('Rows in joined table without where clause: ', len(result))


        agg_table = self.__table_from_rows__('join_derived_table', result)

        result_rows = agg_table.__find_by_template_scan__(where_template, project_fields)
        print('Rows in joined table after where clause applied: ', len(result_rows))

        return result_rows


    def execute_smart_join(self, right_r, on_fields, where_template, project_fields):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.

        """

        # Optimization#1 : Always have the probe as the one with most efficient index. Table swap to choose best access path.

        scan_tbl, probe_tbl = self.choose_scan_probe_table(right_r, on_fields)

        print('Optimization #1: Table swap to choose best access path')

        if scan_tbl != self :
            print('Optimizer log: Tables Swapped')
        else:
            print('Optimizer log: Tables Not Swapped')

        ## Pushdown where clause to scan table
        print('Optimization #2: PushDown the Where clause')
        print("Optimizer log: Number of rows to scan before pushdown = ", len(scan_tbl.__rows__))
        scan_where_template = scan_tbl.get_sub_where_template(where_template)
        scan_project_fields = scan_tbl.get_sub_project_fields(project_fields)
        scan_rows = scan_tbl.find_by_template(scan_where_template, scan_project_fields)
        # print(scan_project_fields)
        print("Optimizer log: Number of rows to scan after pushdown = ", len(scan_rows))

        result = []
        for sr in scan_rows:
            probe_join_template = probe_tbl.__get_on_template__(sr, on_fields)
            probe_where_template = probe_tbl.get_sub_where_template(where_template)
            probe_template = {**probe_join_template, **probe_where_template}
            probe_project_fields = probe_tbl.get_sub_project_fields(project_fields)

            prob_rows = probe_tbl.find_by_template(probe_template, probe_project_fields)

            if prob_rows:
                for pr in prob_rows:
                    joined_row = {**sr, **pr}
                    result.append(joined_row)

        return result


    def choose_scan_probe_table(self, right_r, on_fields):
        left_path, left_count = self.get_access_path(on_fields)
        right_path, right_count = right_r.get_access_path(on_fields)

        if left_path is None and right_path is None:
            return self, right_r
        elif left_path is None and right_path is not None:
            return self, right_r
        elif left_path is not None and right_path is None:
            return right_r, self
        elif right_count < left_count:
            return self, right_r
        else:
            return right_r, self


    def __get_on_template__(self, row, on_fields):
        """
        Gets the on clause as a template for an individual row to easily compare to other table
        :param row: the row that you are creating the template
        :param on_fields: list of fields to join ex: ['playerID', 'teamID']
        :return:
        """
        template = {}
        for field in on_fields:
            value = row[field]
            template[field] = value

        return template

    def get_sub_where_template(self, wt):

        sub_where_template = {}
        table_columns = self.__get_column_names__()

        for key_name in wt.keys():
            if key_name in table_columns:
                sub_where_template[key_name] = wt[key_name]

        return sub_where_template

    def get_sub_project_fields(self, fields):

        cols = self.__get_column_names__()
        project_fields = [value for value in fields if value in cols]

        return project_fields


    def get_row_list(self):
        """
        gets all rows of the table
        :return: List of row dictionaries
        """
        return self.__rows__


    def __table_from_rows__(self, table_name, rows):
        """
        Creates a new instance of CSVTable with a table name and rows passed through (from the join)
        :param table_name: String that is the name of the table
        :param rows: a list of dictionaries that contain row info for the table
        :return: the new table
        """
        new_table = CSVTable(table_name, False)
        new_table.__rows__ = rows
        new_table.__description__ = None

        return new_table



