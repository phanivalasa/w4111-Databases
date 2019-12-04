# Database Architecture
### Phani Valasa (pkv2103)

<br/>

**Project Structure**

The Project folder has been restructured a bit for ease of understanding and better readability. It has four sub folders: data, database, src and tests. The Readme file is under the project folder.

'data' folder as the following files.

* People.csv - data for the People table.
* Apperances.csv - data for the Appearances table.
* Batting.csv - data for the Batting table.
* People_test.csv - sample that I've created from People table for testing the Catalog functions such as Add Index, Add Column, Delete Column etc.,
* People_join_test.csv - sample that I've created from People to test the joins on a smaller dataset.

'database' folder has the metadata (catalog) information of the above tables. Please note that am not storing the metadata/catalog information in the database but rather storing them in the below file formats. It's a dictionary with the keys as table_name, file_name, columns and indexes. Each of the values of these keys is either a string or a list of dictionaries.

* People.metadata - Metadata on People table.
* Apperances.metadata - Metadata on Appearances table
* Batting.metadata - Metadata on Batting table
* People_test.metadata - Metadata on People_test table
* People_join_test.metadata - Metadata on People_join_test table

'src' folder has the following files:

* CSVCatalog.py - It has Class Column Definition, Class Index Definition, Class Table Definition and CSV Catalog Definition. We store the definition information for tables, columns and indexes as part of this file. My implementation of primary key columns is captured as part of PRIMARY Index. Also it has methods to add, update and remove column/index definitions and also to persist the metadata information to a file.
* CSVTable.py - Implementation of CSVTable by using the Catalog created above. It loads the data from the csv files and leverages the corresponding metadata file to create indexes. It has methods provided to find records by template that leverages the best access path. I've implemented a query engine for joins that uses the existing metadata and data.

'tests' folder has the following files:

* unit_test_catalog.py - Test scripts for Catalog. Please see the test strategy section below for details.
* test_catalog_output.txt - Output from the above Catalog test scripts.
* unit_test_csv_table.py - Test scripts for CSV Table joins. Please see the test strategy section below for details.
* test_csv_table_output.txt - Output from the above CSV Table join test scripts.

**Methods implemented in CSVCatalog.py:**
* IndexDefinition - Completed the Index definition class.
* load_columns - Create column definitions from the input values and add it to Table definition columns.
* load_indexes - Create index definitions from the input values and add it to Table definition indexes.
* save_core_definition - Dump the Table definition information to a metadata file.
* load_core_definition - Read the Table definition from the metadata file. It takes support from load_columns and load_indexes.
* add_column_definition - Add new column definition to Table definition columns. Reject duplicate columns
* add_index_definition - Add new index definition to Table definition indexes. Reject duplicate indexes
* get_column - Get column definition using column name.
* drop_column_definition - Drop column definition using column name. Save the changes to metadata file.
* define_primary_key - Add Primary Key for the columns provided. I capture the primary key columns using the primary key index. Replace if a pk already exists. Save the changes to metadata file.
* save_index_definition - Create an index definition based on the columns provided, add it to Table definition and save it to the metadata file.
* define_index - Create an index definition based on the columns provided, add it to Table definition. Reject if the index already exists.
* get_index_cols - Return the list of column names for a given index name.
* get_index - Return the index definition for the given index name.
* drop_index - Drop index definition using index name. Save the changes to metadata file.
* create_table - Create and return a Table Definition object which has the metadata of the table. Table name, data file name, columns definitions and index defintitions are it's inputs.
* drop_table - Drop an existing table definition and the persisted metadata.
* get_table - Load a previously defined table definition from it's metadata.



**Methods implemented in CSVTable.py:**

* dumb_join -- for each row in left table, lookup on every row of right table with the join criteria. If row matches, combine all fields add it to the matched rows. With all such matched rows, create a derived table that can be searched with the where template provided.
* execute_smart_join -- This method kind of rewrites the given query to execute in an optimized way. Implements two key optimization methods. The first chooses the scan and probe tables based on the index availability and selectivity of the indexes. The second one pushes down the where clause to reduce the table size before joining. We also use find_by_template, during the probe phase, which in turn uses the best available index for the lookup.
* get_sub_project_fields  -- additional helper function to obtain fields to be projected only if they exists in the given table.


**Test Strategy for Catalog:**

I've developed scripts for validating the following test scenarios and validated. For each of the test scenario, validated that the metadata file is reflected with the changes correctly. Included the negative cases where applicable.
* Create new table definition and the corresponding metadata file. Include the scripts for all the tables - People, Appearances, Batting and People_test
* Loading a previously defined table definition.
* Dropping an existing table definition. Included the cases to test what happens when the table definition to be dropped doesn't already exist.
* Dropping an existing column.
* Dropping an existing index. Included cases to test when the index do not already exist.
* Adding Primary Key.
* Adding a new Index.


**Test Strategy for CSVTable joins:**

I've developed the scripts for validating the query performance using dumb_join and smart_joins. For each of the test cases, the results are captured in the test output file. Here is the summary..

* Smart Join- Join People table with Appearances table using 'playerId'. Also has a where clause applied. Time taken is 0.015 sec
* Dumb Join - Join People table with Appearances table using 'playerId'. Also has a where clause applied. Time taken is 72.57 sec
* Smart Join- Join People table with Batting table using 'playerId'. Also has a where clause applied. Time taken is 0.19 sec
* Dumb Join - Join People table with Batting table using 'playerId'. Also has a where clause applied. Time taken is 1498.79 sec i.e., 25 min.
