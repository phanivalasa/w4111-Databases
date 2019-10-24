# W4111_F19_HW2
### Phani Valasa 
#### UNI - PKV2103

The Project folder has two sub folders: src and tests. 
The Readme file is under the project folder and so is app.py and 'Test Results.pdf'

src folder has  data_service folder which in turn has the following files:

1) dbutils.py : It has helper functions to create and run sql queries. I've modified the create_select method to include limit and offset 
2) RDBDataTable.py : This is the solution file from HW1. In this file, I've implemented get_row_count and get_primary_key_columns methods.
In addition, find_by_template has been changed for limit and offset functionality required for pagination.
3) data_table_adaptor.py : Implemented the following methods
    * get_databases: Return the list of databases in the local instance. 
    * get_table_names: Return the list of table names for a given database name.
    * get_by_primary_key: Retrieve the data based on the primary key values provided.
    Provides the dataset if obtained else return None.
    * delete_by_primary_key: Delete the record from the database and return the number of rows deleted.
    * update_by_primary_key: 
    Update the record in the database and return the number of rows updated.
    * get_by_template: Retrieve records from database considering offset/limit, where available, and return the resultant dataset.
    * insert_record: Insert record into data and return the number of records inserted. 

Project folders has the following files:  
       
1) app.py : Implemented the following new and modified helper functions:
    * log_and_extract_input: modified this method to implement offset, limit and field params.
    * custom_formatted_output: Most important function for response formatting and pagination. This function does the formatting of the data retrieved by the functions in adaptor file.
    And then it manages the handling of Pagination links. 
    * Implementation of databases GET, table names GET, primary key GET/PUT/DELETE & resource ID GET/POST.    
2) Test Results.pdf : This has the test cases to validate all the above modules. 
Pagination has been incorporated into all the cases where required. 

