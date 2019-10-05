from src.RDBDataTable import RDBDataTable
import logging
import pymysql
import os
import json

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger()
logger.setLevel(logging.ERROR)


cnx_info = {
    "host": "localhost",
    "port": 3306,
    "user": "dbuser",
    "password": "dbuserdbuser",
    "db": "lahman2019raw",
    'charset': 'utf8mb4'
}

# Primary Keys
pk_people = ['playerID']
pk_batting = ['playerID', 'teamID', 'yearID', 'stint']
pk_appearances = ['playerID', 'teamID', 'yearID']

# Test case to validate the initialization, get_row_count, get_connection, str and run_q functions
# The below object creation the constructor, which open a connection to database and runs a query on count to get the total number of records
# and prints 10 sample records.
def t_load_init():

    print("\n\n")
    print("******************** " + "test_load good" + " ********************")

    try:

        rdb_tbl = RDBDataTable("People", connect_info=cnx_info, key_columns=pk_people)
        print("RDB table = ", rdb_tbl)
        print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test0: Exception in load data = ", err)
        print("\n This is the wrong answer")

    print("******************** " + "end test_load_good" + " ********************")

# Test Case to validate find_my_template
def t_find_template_1():

    print("\n\n")
    print("******************** " + "test_find_by_template " + " ********************")

    try:
        rdb_tbl = RDBDataTable("People", connect_info=cnx_info, key_columns=pk_people)
        template = {"birthCity": "Philadelphia", "deathCity": "Philadelphia", "nameFirst": "Charlie"}
        field_list = ['playerID', 'nameFirst', 'nameLast', 'throws', 'deathYear', 'deathMonth']
        print("Template:", json.dumps(template, indent=2))
        print("Desired fields:", field_list)

        result = rdb_tbl.find_by_template(template, field_list)

        print("RDB Table Test1 (Find By Template ): Result = ", json.dumps(result, indent=2), "\nTotal Records:", len(result))
        print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test1: Exception in load data = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_find_by_template" + " ********************")

# Test Case to validate find_my_template with invalid columns in the template; check to see if the exception is raised
def t_find_template_2():

    print("\n\n")
    print("******************** " + "test_find_by_template; Invalid Keys " + " ********************")

    try:
        rdb_tbl = RDBDataTable("people", connect_info=cnx_info, key_columns=pk_people)

        template = {"birthCity1": "Philadelphia", "deathCity": "Philadelphia", "nameFirst": "Charlie"}
        field_list = ['playerID', 'nameFirst', 'nameLast', 'throws', 'deathYear', 'deathMonth']

        print("Template:", json.dumps(template, indent=2))
        print("Desired fields:", field_list)

        result = rdb_tbl.find_by_template(template, field_list)

        print("RDB Table Test1b (Find By Template - Invalid field names in template): Result = ", json.dumps(result, indent=2))
        print("\nThis is the wrong answer")

    except Exception as err:
        print("RDB Test1b: Exception in load data = ", err)
        print("\nThis is the correct answer")

    print("******************** " + "end test_find_by_template; Invalid keys" + " ********************")

# Test Case to validate find_my_template; Desired fields not mentioned so it should all available columns
def t_find_template_3():

    print("\n\n")
    print("******************** " + "test_find_by_template; no desired fields " + " ********************")

    try:
        rdb_tbl = RDBDataTable("People", connect_info=cnx_info, key_columns=pk_people)
        template = {"birthCity": "Philadelphia", "deathCity": "Philadelphia", "nameFirst": "Charlie"}
        print("Template:", json.dumps(template, indent=2))

        result = rdb_tbl.find_by_template(template)

        print("RDB Table Test1c (Find By Template ): Result = ", json.dumps(result, indent=2), "\nTotal Records:", len(result))
        print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test1c: Exception in load data = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_find_by_template; no desired fields" + " ********************")


# Test Case to validate the data with primary key values; the return fields not mentioned so it should return everything
def t_find_primary_key_1():
    print("\n\n")
    print("******************** " + "test_find_by_primary_key; no desired fields " + " ********************")

    try:
        rdb_tbl = RDBDataTable("batting", connect_info=cnx_info, key_columns=pk_batting)
        key_fields = ['aardsda01', 'SEA', '2010', '1']
        print("Table Name - batting")
        print("key fields:", key_fields)

        result = rdb_tbl.find_by_primary_key(key_fields)
        print("RDB Table Test2 (Find By Primary Key - not mentioning the return columns): Result = ", json.dumps(result, indent=2))
        print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test2: Exception in load data = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_find_by_template; no desired fields" + " ********************")

# Test Case to validate the data with primary key values; the return fields mentioned
def t_find_primary_key_2():

    print("\n\n")
    print("******************** " + "test_find_by_template; with desired fields " + " ********************")

    try:

        rdb_tbl = RDBDataTable("batting", connect_info=cnx_info, key_columns=pk_batting)
        field_list = ['playerID','yearID','stint','teamID']
        key_fields = ['aardsda01', 'SEA', '2010', '1']
        print("Table Name - batting")
        print("key fields:", key_fields)
        print("Desired fields:", field_list)

        result = rdb_tbl.find_by_primary_key(key_fields,field_list)

        print("RDB Table Test2b (Find By Primary Key -  with the return columns): Result = ", json.dumps(result, indent=2))
        print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test2b: Exception = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_find_by_template; with desired fields" + " ********************")

# Test Case to validate Inserts when the primary key doesn't exist in the table
def t_insert_1():

    print("\n\n")
    print("******************** " + "test_insert " + " ********************")

    try:

        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)

        print("Table Name - appearances")

        no_rows_before_insert = rdb_tbl._get_row_count()
        print("no of rows before insert:", no_rows_before_insert)

        new_record1 = {"yearID": "2019", "teamID": "SEA", "playerID": "aardsda01", "G_all": "45", "G_batting": "3",
                       "G_defense": "45", "G_p": "45"}
        new_record2 = {"yearID": "2001", "teamID": "SEA", "playerID": "aardsda01", "G_all": "32", "G_batting": "7",
                       "G_defense": "32", "G_p": "32"}
        new_record3 = {"yearID": "2002", "teamID": "SEA", "playerID": "aardsda01", "G_all": "30", "G_batting": "9",
                       "G_defense": "30", "G_p": "30"}
        new_record4 = {"yearID": "2003", "teamID": "SEA", "playerID": "aardsda01", "G_all": "30", "G_batting": "9",
                       "G_defense": "30", "G_p": "30"}

        rdb_tbl.insert(new_record1)
        result = rdb_tbl.find_by_template(new_record1)
        print("RDB Test3 (Insert): Result from find_my_template = ", json.dumps(result, indent=2))

        rdb_tbl.insert(new_record2)
        result = rdb_tbl.find_by_template(new_record2)
        print("RDB Test3 (Insert): Result from find_my_template= ", json.dumps(result, indent=2))

        rdb_tbl.insert(new_record3)
        result = rdb_tbl.find_by_template(new_record3)
        print("RDB Test3 (Insert): Result from find_my_template= ", json.dumps(result, indent=2))

        rdb_tbl.insert(new_record4)
        result = rdb_tbl.find_by_template(new_record4)
        print("RDB Test3 (Insert): Result from find_my_template= ", json.dumps(result, indent=2))

        no_rows_after_insert = rdb_tbl._get_row_count()
        print("no of rows after insert:", no_rows_after_insert)
        print("no of rows Inserted successfully:", no_rows_after_insert - no_rows_before_insert)
        print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test3: Exception  = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_insert" + " ********************")


# Test Case to validate Inserts when the primary key already exists; Should throw an exception
def t_insert_2():

    print("\n\n")
    print("******************** " + "test_insert; PK already exists " + " ********************")

    try:
        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)
        print("Table Name - appearances")

        no_rows_before_insert = rdb_tbl._get_row_count()
        print("no of rows before insert:", no_rows_before_insert)

        new_record1 = {"yearID": "2019", "teamID": "SEA", "playerID": "aardsda01", "G_all": "45", "G_batting": "3",
                       "G_defense": "45", "G_p": "45"}

        rdb_tbl.insert(new_record1)

        # The below flow won't be executed because of the Duplicate primary key exception
        no_rows_after_insert = rdb_tbl._get_row_count()
        print("no of rows after insert:", no_rows_after_insert)
        print("\nThis is the wrong answer")

    except Exception as err:
        print("RDB Test3b: Exception in Insert  = ", err)
        print("\nThis is the correct answer")

    print("******************** " + " end test_insert; PK already exists " + " ********************")

# Test Case to validate Delete by template
def t_delete_by_template_1():
    print("\n\n")
    print("******************** " + "test_delete_by_template " + " ********************")

    try:
        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)
        template = {"playerID":"aardsda01", "G_all":"45", "G_batting":"3"}
        print("Table Name - appearances")
        print("Template:", json.dumps(template, indent=2))

        del_count = rdb_tbl.delete_by_template(template)
        print("CSV Table Test4; Records Deleted : ",del_count)

        result = rdb_tbl.find_by_template(template)  # Check if Delete was successful
        if result:
            print("CSV Table Test4: Unsuccessfull = Records found by template after delete")
            print("\nThis is the wrong answer")
        else:
            print("CSV Table Test4: Successfull = Records not found by template after delete")
            print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test4: Exception in Delete by template = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_delete_by_template " + " ********************")

# Test Case to validate Delete by key fields
def t_delete_by_key_1():
    print("\n\n")
    print("******************** " + "test_delete_by_key " + " ********************")

    try:
        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)
        key_fields = ['aardsda01', 'SEA', '2001']

        print("Table Name - appearances")
        print("Key Fields:", key_fields)

        del_count = rdb_tbl.delete_by_key(key_fields)
        print("CSV Table Test5; Records Deleted : ",del_count)

        # no_rows_after_delete = rdb_tbl._get_row_count()
        # print("total no of rows after delete:", no_rows_after_delete)

        result = rdb_tbl.find_by_primary_key(key_fields)  # Check if Delete was successful
        if result:
            print("CSV Table Test5: Unsuccessfull = Records found by key after delete")
            print("\nThis is the wrong answer")
        else:
            print("CSV Table Test5: Successfull = Records not found by template after delete")
            print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test5: Exception  = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_delete_by_key " + " ********************")

# Test Case to validate update by template
def t_update_by_template_1():
    print("\n\n")
    print("******************** " + "test_update_by_template " + " ********************")

    try:
        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)

        template = {"playerID": "aardsda01", "G_all": "30"}
        new_values = {"G_batting": "90", "G_defense": "100", "G_p": "100"}
        field_list = ['playerID','teamID','yearID','G_all','G_batting','G_defense','G_p']

        print("Table Name - appearances")
        print("Template:", json.dumps(template, indent=2))
        print("New Values:", json.dumps(new_values, indent=2))

        result = rdb_tbl.find_by_template(template, field_list)
        print("Expected no. of records to update:", len(result))
        print("RDB Test6 (Records before udpate_by_template = ", json.dumps(result, indent=2))

        update_count = rdb_tbl.update_by_template(template, new_values)
        print("No. of records udpated:", update_count)

        result = rdb_tbl.find_by_template(template,field_list)  # Values after Update
        print("RDB Test6 (Records after udpate_by_template = ", json.dumps(result, indent=2))
        print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test6: Exception  = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_update_by_template " + " ********************")


# Test Case to validate update by template when new values match with existing PK; Should raise an exception
def t_update_by_template_2():
    print('\n\n')
    print("******************** " + " test_update_by_template; PK already exists " + " ********************")
    try:
        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)

        template = {"playerID": "aardsda01", "teamID": "SEA", "yearID": "2002", "G_all": "30"}
        new_values = {"G_c": "28", "yearID": "2003"}

        print("Table Name - appearances")
        print("Template:", json.dumps(template, indent=2))
        print("New Values:", json.dumps(new_values, indent=2))

        update_count = rdb_tbl.update_by_template(template, new_values)
        print("RDB Test6b (Records updated with PK; Exception should have been raised)", update_count)
        print("\nThis is the wrong answer")

    except Exception as err:
        print("RDB Test6b: Exception in update_by_template = ", err)
        print("\nThis is the correct answer")

    print("******************** " + "end test_update_by_template; PK already exists " + " ********************")

# Test Case to validate update by primary key
def t_update_by_key_1():
    print("\n\n")
    print("******************** " + "test_update_by_key " + " ********************")

    try:
        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)

        key_fields = ['aardsda01', 'SEA', '2003']
        new_values = {"G_batting":"30", "G_defense":"240", "G_p":"30"}
        field_list = ['playerID', 'teamID', 'yearID', 'G_all', 'G_batting', 'G_defense', 'G_p']

        print("Table Name - appearances")
        print("Keys:", key_fields)
        print("New Values:", json.dumps(new_values, indent=2))

        result = rdb_tbl.find_by_primary_key(key_fields, field_list)
        print("Expected no. of records to update:", len(result))
        print("CSV Test7 (Records before udpate_by_key = ", json.dumps(result, indent=2))

        update_count = rdb_tbl.update_by_key(key_fields, new_values)
        print("No. of records udpated:", update_count)

        result = rdb_tbl.find_by_primary_key(key_fields,field_list)  # Values after Update
        print("CSV Test7 (Records after udpate_by_key = ", json.dumps(result, indent=2))
        print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test7: Exception  = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_update_by_key " + " ********************")

# Test Case to validate update by primary key to an existing another record key; should raise an exception
def t_update_by_key_2():
    print('\n\n')
    print("******************** " + " test_update_by_key; PK already exists " + " ********************")

    try:
        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)

        key_fields = ['aardsda01', 'SEA', '2002']
        new_values = {"G_all":"240", "yearID":"2003", "teamID":"SEA", "G_batting":"30", "G_defense":"240", "G_p":"30"}
        field_list = ['playerID', 'teamID', 'yearID', 'G_all', 'G_batting', 'G_defense', 'G_p']

        print("Table Name - appearances")
        print("Keys:", key_fields)
        print("New Values:", json.dumps(new_values, indent=2))

        result = rdb_tbl.find_by_primary_key(key_fields, field_list)
        print("Expected no. of records to update:", len(result))

        update_count = rdb_tbl.update_by_key(key_fields, new_values)
        print("No. of records udpated:", update_count)
        print("\nThis is the wrong answer")

    except Exception as err:
        print("RDB Test7b: Exception in update_by_key = ", err)
        print("\nThis is the correct answer")

    print("******************** " + "end test_update_by_key; PK already exists " + " ********************")

# Test the get_rows function; optional
def t_get_rows():
    print('\n\n')
    print("******************** " + " test_get_rows; optional " + " ********************")

    try:
        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)
        print("Table Name - appearances")
        result = rdb_tbl.get_rows()
        print("RDB Test8 - Rows read:", len(result))
        print("\nThis is the correct answer")

    except Exception as err:
        print("RDB Test8: Exception in get_rows = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_get_rows " + " ********************")


def del_inserted_rows():
    print('\n\n')
    print("******************** " + " Delete the extra records inserted for this test; cleanup " + " ********************")

    try:
        rdb_tbl = RDBDataTable("appearances", connect_info=cnx_info, key_columns=pk_appearances)
        key_fields = ['aardsda01', 'SEA', '2002']
        print("Table Name - appearances")
        del_count = rdb_tbl.delete_by_key(key_fields)
        if del_count > 0:
            print('Record deleted by key:',key_fields)
        else:
            print('No rows deleted')

        key_fields = ['aardsda01', 'SEA', '2003']
        del_count = rdb_tbl.delete_by_key(key_fields)
        if del_count > 0:
            print('Record deleted by key:', key_fields)
        else:
            print('No rows deleted')

    except Exception as err:
        print("RDB Test9: Exception in get_rows = ", err)

    print("*************** " + " Delete the extra records inserted for this test; cleanup " + " *****************")


t_load_init()  # 0; Test case to validate the initialization, get_row_count, get_connection, str and run_q functions
t_find_template_1() # 1; Test Case to validate find_my_template
t_find_template_2() # 1b; Test Case to validate find_my_template with invalid columns in the template;
t_find_template_3() #1c; Test Case to validate find_my_template; no desired fields


t_find_primary_key_1() ## TEST 2 : Test Case to validate the data with primary key values; the return fields not mentioned
t_find_primary_key_2() ## TEST 2b : Test Case to validate the data with primary key values; the return fields mentioned

t_insert_1() ## TEST 3: Test Case to validate Inserts when the primary key doesn't exist in the table
t_insert_2() ## TEST 3b: Test Case to validate Inserts when the primary key already exists; Should throw an exception

t_delete_by_template_1() ## TEST 4: Test Case to validate Delete by template

t_delete_by_key_1() ## TEST 5: Test Case to validate Delete by key fields

t_update_by_template_1() ## TEST 6: Test Case to validate update by template
t_update_by_template_2() ## TEST 6b: Test Case to validate update by template when new values match with existing PK;

t_update_by_key_1() ## TEST 7: Test Case to validate update by primary key
t_update_by_key_2() ## TEST7b: Test Case to validate update by primary key to an existing another record key;

t_get_rows() ## TEST 8: Test the get_rows function; optional

del_inserted_rows() ## TEST 9: deleted additional records to cleanup the dataset for next run; optional



