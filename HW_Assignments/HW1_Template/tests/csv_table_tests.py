# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os
import json


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.ERROR)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


# Primary Keys
pk_people = ['playerID']
pk_batting = ['playerID', 'teamID', 'yearID', 'stint']
pk_appearances = ['playerID', 'teamID', 'yearID']

# Test Case to validate the data load
def t_load():

    print("\n\n")
    print("******************** " + "test_load_good" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    try:
        csv_tbl = CSVDataTable("batting", connect_info, None)
        print("Created table = " + str(csv_tbl))
        print("\n This is the correct answer")

    except Exception as err:
        print("RDB Test0: Exception in load data = ", err)
        print("\n This is the wrong answer")

    print("******************** " + "end test_load_good" + " ********************")


# Test Case to validate find_my_template
def t_find_template_1():

    print("\n\n")
    print("******************** " + "test_find_by_template good" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    try:
        csv_tbl = CSVDataTable("people", connect_info, None)

        template = {"birthCity": "Philadelphia", "deathCity": "Philadelphia", "nameFirst": "Charlie"}
        field_list = ['playerID', 'nameFirst', 'nameLast', 'throws', 'deathYear', 'deathMonth']
        result = csv_tbl.find_by_template(template, field_list)

        print("CSV Table Test1 (Find By Template ): Result = ", json.dumps(result, indent=2), "\nTotal Records:",
              len(result))
        print("\nThis is the correct answer")

    except Exception as err:
        print("CSV Test1: Exception in find_by_template = ", err)
        print("\n This is the wrong answer")

    print("******************** " + "end test_find_by_template good" + " ********************")

# Test Case to validate find_my_template; There are no desired fields set, so all the available columns should be displayed
def t_find_template_1b():

    print("\n\n")
    print("******************** " + "test_find_by_template when but no desired fields mentioned" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    try:
        csv_tbl = CSVDataTable("people", connect_info, None)

        template = {"birthCity": "Philadelphia", "deathCity": "Philadelphia", "nameFirst": "Charlie"}
        result = csv_tbl.find_by_template(template)

        print("CSV Table Test1 (Find By Template ): Result = ", json.dumps(result, indent=2), "\nTotal Records:",
              len(result))
        print("\nThis is the correct answer; The output should have all the available columns")

    except Exception as err:
        print("CSV Test1: Exception in find_by_template = ", err)
        print("\n This is the wrong answer")

    print("******************** " + "end test_find_by_template when but no desired fields mentioned" + " ********************")



# Test Case to validate find_my_template with invalid keys in the template; check to see if the exception is raised
def t_find_template_2():

    print("\n\n")
    print("******************** " + "test_find_by_template invalid_names_in_template" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    try:
        csv_tbl = CSVDataTable("people", connect_info, None)

        template = {"birthCity1": "Philadelphia", "deathCity": "Philadelphia", "nameFirst": "Charlie"}
        field_list = ['playerID', 'nameFirst', 'nameLast', 'throws', 'deathYear', 'deathMonth']
        result = csv_tbl.find_by_template(template, field_list)

        print("CSV Table Test2 (Find By Template - Invalid field names in template): Result = ", json.dumps(result, indent=2))
        print("\n This is the wrong answer")
    except Exception as err:
        print("CSV Test2: Exception in find_by_template = ", err, "; Template - ", json.dumps(template, indent=2) )
        print("\nThis is the correct answer")

    print("******************** " + "end test_find_by_template invalid_names_in_template" + " ********************")

# Test Case to validate find_my_template with invalid keys in the field_list; check to see if the exception is raised
def t_find_template_3():

    print("\n\n")
    print("******************** " + "test_find_by_template invalid_names_in_template" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    try:
        csv_tbl = CSVDataTable("people", connect_info, None)

        template = {"birthCity": "Philadelphia", "deathCity": "Philadelphia", "nameFirst": "Charlie"}
        field_list = ['playerID1', 'nameFirst', 'nameLast', 'throws', 'deathYear', 'deathMonth']
        result = csv_tbl.find_by_template(template, field_list)

        print("CSV Table Test2b (Find By Template - Invalid field names): Result = ", json.dumps(result, indent=2))
        print("\n This is the wrong answer")
    except Exception as err:
        print("CSV Test2b: Exception in find_by_template = ", err, "; Desired Fields - ", field_list )
        print("\nThis is the correct answer")

    print("******************** " + "end test_find_by_template invalid_names_in_template" + " ********************")



# Test Case to validate the data with primary key values; the return fields not mentioned so it should return everything
def t_find_primary_key_1():

    print("\n\n")
    print("******************** " + "test_find_by_primary_key good but no mention of desired fields" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=pk_batting)
        key_fields = ['aardsda01', 'SEA', '2010', '1']
        result = csv_tbl.find_by_primary_key(key_fields)


        print("CSV Table Test3 (Find By Primary Key - not mentioning the return columns): Result = ", json.dumps(result, indent=2))
        print("\nThis is the correct answer")

    except Exception as err:
        print("CSV Test3: Exception in find_by_primary_key = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_find_by_primary_key good but no mention of desired fields" + " ********************")

# Test Case to validate the data with primary key values; the return fields mentioned
def t_find_primary_key_2():

    print("\n\n")
    print("******************** " + "test_find_by_primary_key with desired fields" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=pk_batting)
        field_list = ['playerID','yearID','stint','teamID']
        key_fields = ['aardsda01', 'SEA', '2010', '1']
        result = csv_tbl.find_by_primary_key(key_fields,field_list)

        print("CSV Table Test3b (Find By Primary Key -  with the return columns): Result = ", json.dumps(result, indent=2))
        print("\nThis is the correct answer")

    except Exception as err:
        print("CSV Test3b: Exception in find_by_primary_key = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_find_by_primary_key with desired fields"+ " ********************")


# Test Case to validate the data with primary key values; the values given do not exist in table; should return no rows
def t_find_primary_key_3():
    print("\n\n")
    print("******************** " + "test_find_by_primary_key with no matching rows" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    try:
        csv_tbl = CSVDataTable("batting", connect_info, key_columns=pk_batting)
        field_list = ['playerID', 'yearID', 'stint', 'teamID']
        key_fields = ['aardsda01X', 'SEA', '2010', '1']
        result = csv_tbl.find_by_primary_key(key_fields, field_list)

        print("CSV Table Test3c (Find By Primary Key -  with the return columns): Result = ",
              json.dumps(result, indent=2))
        print("\nThis is the correct answer")

    except Exception as err:
        print("CSV Test3c: Exception in find_by_primary_key = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_find_by_primary_key with no matching rows" + " ********************")


# Test Case to validate Inserts when the primary key doesn't exist in the table
def t_insert_1():

    print("\n\n")
    print("******************** " + "test_inserts; primary key doesn't exist in the table already " + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    try:
        csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_appearances)
        new_record = {"yearID": "2019", "teamID": "SEA", "lgID": "AL", "playerID": "aardsda01", "G_all": "53",
                      "GS": "0", "G_batting": "4", "G_defense": "53", "G_p": "53", "G_c": "0", "G_1b": "0", "G_2b": "0",
                      "G_3b": "0", "G_ss": "0", "G_lf": "0",
                      "G_cf": "0", "G_rf": "0", "G_of": "0", "G_dh": "0", "G_ph": "0", "G_pr": "0"}

        csv_tbl.insert(new_record)
        result = csv_tbl.find_by_template(new_record)
        if result:
            print("CSV Table Test4 (Insert Successful-  with no prior data for the primary key")
            print("\nThis is the correct answer")
        else:
            print("CSV Table Test4 (Insert Failed -  with no prior data for the primary key")
            print("\nThis is the wrong answer")

    except Exception as err:
        print("CSV Test4: Exception in insert = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "end test_inserts; primary key doesn't exist in the table already" + " ********************")

# Test Case to validate Inserts FAIL when the primary key already exists
def t_insert_2():

    print("\n\n")
    print("******************** " + "test_inserts; primary key already exist; insert should FAIL " + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    try:
        csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_appearances)
        new_record = {"yearID": "2019", "teamID": "SEA", "lgID": "AL", "playerID": "aardsda01", "G_all": "53",
                      "GS": "0", "G_batting": "4", "G_defense": "53", "G_p": "53", "G_c": "0", "G_1b": "0", "G_2b": "0",
                      "G_3b": "0", "G_ss": "0", "G_lf": "0",
                      "G_cf": "0", "G_rf": "0", "G_of": "0", "G_dh": "0", "G_ph": "0", "G_pr": "0"}

        csv_tbl.insert(new_record)

        csv_tbl.insert(new_record)
        result = csv_tbl.find_by_template(new_record)
        if result:
            print("CSV Table Test4b (Insert Successful-  with prior data for the primary key")
            print("\nThis is the wrong answer")
        else:
            print("CSV Table Test4b (Insert Failed -  with prior data for the primary key")
            print("\nThis is the wrong answer")

    except Exception as err:
        print("CSV Test4b: Exception in insert = ", err)
        print("\nThis is the correct answer")

    print("******************** " + "end test_inserts; primary key already exist; insert should FAIL" + " ********************")


# Test Case to validate Delete by template
def t_delete_by_template_1():

    print("\n\n")
    print("******************** " + "test_delete_by_template " + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    try:
        csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_appearances)
        template = {"playerID": "aardsda01"}

        del_count = csv_tbl.delete_by_template(template)
        print("CSV Table Test5; Records Deleted : ",del_count, "\n Table Name - appearances",
              "\n Template -", json.dumps(template, indent=2) )

        result = csv_tbl.find_by_template(template)  # Check if Delete was successful
        if result:
            print("CSV Table Test5: Result = Delete Unsuccessful")
            print("\nThis is the wrong answer")
        else:
            print("CSV Table Test5: Result = Records not found after deletion")
            print("\nThis is the correct answer")


    except Exception as err:
        print("CSV Test5: Exception in insert = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "test_delete_by_template" + " ********************")

# Test Case to validate Delete by template; with non-existing values in the table
def t_delete_by_template_2():

    print("\n\n")
    print("******************** " + "test_delete_by_template; with non-existing values in the table " + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    try:
        csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_appearances)
        template = {"playerID": "aardsda01X"}

        del_count = csv_tbl.delete_by_template(template)
        print("CSV Table Test5; Records Deleted : ",del_count, "\n Table Name - appearances",
              "\n Template -", json.dumps(template, indent=2) )

        result = csv_tbl.find_by_template(template)  # Check if Delete was successful
        if result:
            print("CSV Table Test5b: Result = Delete Unsuccessful")
            print("\nThis is the wrong answer")
        else:
            print("CSV Table Test5b: Result = Records not found after deletion")
            print("\nThis is the correct answer")


    except Exception as err:
        print("CSV Test5b: Exception in  = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "test_delete_by_template; with non-existing values in the table" + " ********************")

# Test Case to validate Delete by key fields
def t_delete_by_key_1():

    print("\n\n")
    print("******************** " + "delete_by_key" + " ********************")
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    try:
        csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_appearances)
        key_fields = ['aardsda01', 'SEA', '2009']

        del_count = csv_tbl.delete_by_key(key_fields)
        print("CSV Table Test6; Records Deleted : ", del_count, "\n Table Name - appearances",
              "\n key_fields -", key_fields)

        result = csv_tbl.find_by_primary_key(key_fields)  # Check if Delete was Unsuccessful
        if result:
            print("CSV Table Test6: Result = Delete Unsuccessful")
            print("\nThis is the wrong answer")
        else:
            print("CSV Table Test6: Result = Records not found after deletion")
            print("\nThis is the correct answer")

    except Exception as err:
        print("CSV Test6: Exception in  = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "test_delete_by_key" + " ********************")

# Test Case to validate update by template
def t_update_by_template_1():

    print("\n\n")
    print("******************** " + "update_by_template" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    try:
        csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_appearances)

        template = {"playerID": "aardsda01", "teamID": "SEA"}
        new_values = {"G_all": "100", "G_batting": "9", "G_defense": "100", "G_p": "100"}
        print("Template:", json.dumps(template, indent=2))
        print("New Values:", json.dumps(new_values, indent=2))

        result = csv_tbl.find_by_template(template)
        print("Expected no. of records to update:", len(result))
        print("CSV Test7 (Records before udpate_by_template = ", json.dumps(result, indent=2))

        update_count = csv_tbl.update_by_template(template, new_values)
        print("No. of records udpated:", update_count)

        result = csv_tbl.find_by_template(template)  # Values after Update
        print("CSV Test7 (Records after udpate_by_template = ", json.dumps(result, indent=2))
        print("\nThis is the correct answer")

    except Exception as err:
        print("CSV Test7: Exception = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "test_update_by_template" + " ********************")

# Test Case to validate update by template to existing PK values
def t_update_by_template_2():

    print("\n\n")
    print("******************** " + "update_by_template to existing PK Values" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    try:
        csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_appearances)

        template = {"playerID": "aardsda01", "teamID": "SEA", "yearID": "2009"}
        new_values = {"teamID": "BOS", "yearID": "2008"}
        print("Template:", json.dumps(template, indent=2))
        print("New Values:", json.dumps(new_values, indent=2))

        update_count = csv_tbl.update_by_template(template, new_values)
        print("CSV Test7b (Records updated with PK; Exception should have been raised)", update_count)
        print("\nThis is the wrong answer")

    except Exception as err:
        print("CSV Test7b: Exception in update_by_template= ", err)
        print("\nThis is the correct answer")

    print("******************** " + "test_update_by_template to existing PK Values" + " ********************")


# Test Case to validate update by primary key
def t_update_by_key_1():

    print("\n\n")
    print("******************** " + "update_by_key" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    try:
        csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_appearances)

        key_fields = ['aardsda01', 'BOS', '2008']
        new_values = {"G_all":"240", "G_batting":"30", "G_defense":"240", "G_p":"30"}
        print("Keys:", key_fields)
        print("New Values:", json.dumps(new_values, indent=2))

        result = csv_tbl.find_by_primary_key(key_fields)
        print("Expected no. of records to update:", len(result))
        print("CSV Test8 (Records before udpate_by_key = ", json.dumps(result, indent=2))

        update_count = csv_tbl.update_by_key(key_fields, new_values)
        print("No. of records udpated:", update_count)

        result = csv_tbl.find_by_primary_key(key_fields)  # Values after Update
        print("CSV Test8 (Records after udpate_by_key = ", json.dumps(result, indent=2))
        print("\nThis is the correct answer")

    except Exception as err:
        print("CSV Test8: Exception = ", err)
        print("\nThis is the wrong answer")

    print("******************** " + "test_update_by_key" + " ********************")

# Test Case to validate update by primary key to an  another existing  key
def t_update_by_key_2():

    print("\n\n")
    print("******************** " + "update_by_key to existing PK Values" + " ********************")

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    try:
        csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_appearances)

        key_fields = ['aardsda01', 'BOS', '2008']
        new_values = {"G_all":"240", "yearID":"2009", "teamID":"SEA", "G_batting":"30", "G_defense":"240", "G_p":"30"}
        print("Keys:", key_fields)
        print("New Values:", json.dumps(new_values, indent=2))

        result = csv_tbl.find_by_primary_key(key_fields)
        print("Expected no. of records to update:", len(result))
        # print("CSV Test12 (Records before udpate_by_template = ", json.dumps(result, indent=2))

        update_count = csv_tbl.update_by_key(key_fields, new_values)
        print("No. of records udpated:", update_count)
        print("\nThis is the wrong answer")

    except Exception as err:
        print("CSV Test8b: Exception in update_by_key = ", err)
        print("\nThis is the correct answer")

    print("******************** " + "test_update_by_key to existing PK Values" + " ********************")


def t_save_file():

    print("******************** " + "test_save_file to disk" + " ********************")
    connect_info = {
        "directory": data_dir,
        "file_name": "TeamsFranchises.csv"
    }
    pk_team =['franchID']
    new_values = {"NAassoc": "UPD"}
    revert_values = {"NAassoc": ""}

    csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_team)
    key_fields = ['WST']
    field_list = ['franchID',"franchName","active","NAassoc"]

    result = csv_tbl.find_by_primary_key(key_fields,field_list)
    print("Records before udpate_by_key = ", json.dumps(result, indent=2))

    result = csv_tbl.update_by_key(key_fields,new_values)
    print("No. of records updated:", result)

    result = csv_tbl.find_by_primary_key(key_fields,field_list)
    print("Records after udpate_by_key = ", json.dumps(result, indent=2))

    csv_tbl.save()

    saved_csv_tbl = CSVDataTable("appearances", connect_info, key_columns=pk_team)
    result = saved_csv_tbl.find_by_primary_key(key_fields, field_list)
    print("Records from saved file = ", json.dumps(result, indent=2))
    print("If the NAassoc is reflected as 'UPD', the save file is working fine")
    print("\nThis is the correct answer")

    print("******************** " + "end test_save_file to disk" + " ********************")


t_load()
t_find_template_1()    # CSV Table Test1 (Find By Template, field list provided)
t_find_template_1b()    # CSV Table Test1 (Find By Template, field list not provided)
t_find_template_2()    # CSV Table Test2 (Find By Template - Invalid field names in template)
t_find_template_3()    # CSV Table Test2b (Find By Template - Invalid field names in field_list)

t_find_primary_key_1() # CSV Table Test3 (Find By Primary Key - not mentioning the return columns)
t_find_primary_key_2() # CSV Table Test3b (Find By Primary Key -  with the return columns
t_find_primary_key_3() # CSV Table Test3c(Find By Primary Key -  with no matching rows

t_insert_1()           # CSV Table Test4 (Insert -  when no prior data for the primary key)
t_insert_2()           # CSV Table Test4b (Insert -  when primary key already exists)

t_delete_by_template_1() # CSV Table Test5 (Delete by template)
t_delete_by_template_2() # CSV Table Test5 (Delete by template with no matching criteria)

t_delete_by_key_1() # CSV Table Test6 (Delete by key)

t_update_by_template_1() # CSV Table Test7 (Update by template)
t_update_by_template_2() # CSV Table Test7b (Update by template to existing PK values; Exception should be raised)

t_update_by_key_1() # CSV Table Test8 (Update by Key)
t_update_by_key_2() # CSV Table Test8b (Update by Key to an existing key; Should raise an exception)
t_save_file()

