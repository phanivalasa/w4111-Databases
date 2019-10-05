# W4111_F19_HW1
### Phani Valasa 
#### UNI - PKV2103

The Project folder has three sub folders: Data, src and tests. The Readme file is under the project folder.

Data folder has all the data files imported from github page of this assignment. 

src folder has the following files:
* BaseDataTable.py : It has abstract methods of our implementations. No changes made here.
* CSVDataTable.py : It has helper functions as well as the implementation of requested methods in CSVDataTable. It uses the excel files as data source.
* RDBDataTable.py : It has helper functions as well as the implementation of requested methods in RDBDataTable. It uses the MySQL database as data source.
* CaptureException.py : Created a custom class to raise standard exceptions.

tests folder has the following files:
* csv_fast_pk_test.py : It has the test script for the extra credit question that was asked for implementing primary key index on CSV table. I’ve used dictionary structure to store the composite keys and the corresponding records.
* csv_fast_pk_test.txt: The output of performance test with primary key fast implementation. The time taken is 71 secs for 1000 iterations i.e., without indexing. With the fast key implementation, it is 0.001 secs. 
* csv_table_tests.py: Test scripts for validating implementation of all functions. I’ve incorporated the validation of all methods. The test scripts are distributed across batting and appearances tables.
* csv_table_tests.txt: Output from the above file.
* rdb_table_tests.py: Test scripts for validating implementation of all RDB functios. The test scripts are distributed across batting and appearances tables.
* rdb_table_tests.txt: Output of RDBTable test scripts.
* unit_tests.py : It’s the original file from github used as a reference. It hasn’t been modified.


Other Design Considerations:

* While loading the data in CSVDataTable from csv files, am not checking for duplicates using primary keys. Assumption I’ve taken is that the primary keys are chosen carefully before the data in being imported. The similar care has been taken in MySQL database as well to define the primary keys.
* Duplicate checks on primary keys have been implemented in insert and update methods.
* The by_template methods are created first and are referred by all by_key methods.
* The save method saves the modified file back to the disk. I’ve a test script to demonstrate the functioning of this method.
* For creating the index for primary key data retrieval, I’ve used a dictionary object where the composite values of primary key columns is used as a KEY to store the record. This is one way of implementing the indexing which has a constant time (i.e., 1) in retrieval.
* Haven’t implemented the foreign key constraints; This will require checking of child records before delete or update.
* In the current design, the primary keys will need to be passed during initialization of RDBDataTable. And when querying using the primary key, the order of the values passed should be in the same order as was used during the initialization.
* I’ve used appearances, batting and people data files for my test scripts. The primary keys on these tables are chosen as ['playerID', 'teamID', 'yearID'], ['playerID', 'teamID', 'yearID', 'stint'] and [‘playerID’]
* There are many helper methods used in the process of this implementation as mentioned below.

Here is a very high level summary of helper functions and the CRUD operation implementations.

**CSVDataTable.py**


* __init__ : Constructor that sets the object parameters such as table name, connect info, primary key and Loads the data.
* __str__  : It read the the first 5 and last 5 records of the table and displays them.
* _add_row : Helper function to add row to the table.
* _load    : Helper function to load the data into object memory from excel files
* _load_pk_index : This function creates an index based on primary key values. It uses a dictionary object. It is a duplication of memory but helps in faster retrieval.
* save : This helper function saves the modified tables back to the disk overriding the old files.
* check_headers: This checks if the desired fields for display are part of the table headers or not. Returns a boolean
* matches_template: This function checks if the template provided matches with the current row or not. Returns a boolean.
* find_by_fast_key : It does the lookup on the primary key index using composite key. returns dictionary of the record found. It is called by find_by_primary_key_fast

* find_by_primary_key : It looks up the table using the primary key values provided. The order of the primary key values should be in the same order of the primary key defined during initialization.
* find_by_primary_key_fast : Same functionality as above but instead of looping through all the records, this method does a lookup on the index.
* find_by_template : Fetch records based on a dictionary of the form { "field1" : value1, "field2": value2, ...}. Returns a list of dictionaries/records found for the criteria.
* delete_by_key : Delete the row matching the primary key values ( i.e., a list) provided. The order in which the key values are passed is important.
* delete_by_template: Delete the rows determined by the template.
* update_by_key : Update the rows based on the list of primary key values provided. It also ensures the resultant data doesn't violate the primary key constraints.
* update_by_template : Update the rows matching the template. Ensures primary key uniqueness during the process of update.
* insert : Insert the data into table when the new record doesn't violate key constraints.
* get_rows : helper functions to retrieve all rows from the table.


**RDBDataTable.py**

* __init__ : Constructor that sets the object parameters such as table name, database connection and primary key tracking.
* __str__ : It read the the first 10 records of the table and displays them.
* _get_row_count : Returns the count of records in the table 
* _get_connection : helper function to open a connection to the database using the connection information provided during initialization.
* _run_q : Helper function to run a SQL statement.An RDBDataTable MUST have a connection specified by the connection information.
            This implementation of run_q MUST NOT try to obtain a default connection.
* _convert_template_for_query : This helper converts the template provided to a "where clause" and "arguments" that can be used later in "_run_q" function.
* _insert_func: Forms the sql query to insert and executes it using "_run_q" function.

* find_by_primary_key :It looks up the table using the primary key values provided. The order of the primary key values should be in the same order of the primary key defined during initialization.
* find_by_template: Fetch records based on a dictionary of the form { "field1" : value1, "field2": value2, ...}. Returns a list of records found for the criteria.
* delete_by_key: Delete the row matching the primary key values ( i.e., a list) provided. The order in which the key values are passed is important.
* delete_by_template: Delete the rows determined by the template.
* update_by_key: Update the rows based on the list of primary key values provided. Constraints are enforced by database and an excpetion returned for violations.
* update_by_template: Update the rows matching the template. Ensures primary key uniqueness during the process of update, this is done by Database here.
* insert: Insert the data into table when the new record doesn't violate key constraints.
* get_rows: helper functions to retrieve all rows from the table.

