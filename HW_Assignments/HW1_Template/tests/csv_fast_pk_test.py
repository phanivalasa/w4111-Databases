# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os
import json
import time

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


# Primary Keys
pk_people = ['playerID']
pk_batting = ['playerID', 'teamID', 'yearID', 'stint']
pk_appearances = ['playerID', 'teamID', 'yearID']

# Test Case to validate the performance of primary_key_fast implementation
def t_find_primary_key_fast():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    print("\n\n")
    print("**************** " + "Check Performance Improvement using Fask Primary Key search" + " ****************")

    csv_tbl = CSVDataTable("batting", connect_info, key_columns=pk_batting)
    key_fields = ['aardsda01', 'SEA', '2010', '1']

    iterations = 1000

    print("Number of iterations for checking the retrieval performance = ", iterations, '\n')
    print("Primary Key Method - Now loop through the iteration to find out total time taken for: ", key_fields, '\n')
    start_time = time.time()
    for i in range(iterations):
        result = csv_tbl.find_by_primary_key(key_fields)
        if i == 0:
            print("The output is = ",json.dumps(result, indent=2))
    end_time = time.time()
    time_taken1 = end_time - start_time
    print("Total Time Taken using NORMAL primary key retrieval method = ", str(time_taken1),'\n')

    print("Fast Primary Key Method - Now loop through the iteration to find out total time taken for: ", key_fields, '\n')
    start_time = time.time()
    for i in range(iterations):
        result = csv_tbl.find_by_primary_key_fast(key_fields)
        if i == 0:
            print("The output is = ",json.dumps(result, indent=2))
    end_time = time.time()
    time_taken2 = end_time - start_time
    print("Total Time Taken using FAST primary key retrieval method = ", str(time_taken2),'\n')

    print("Total Time Taken using Normal primary key = ", str(time_taken1) , 'secs and Fast Primary Key =', str(time_taken2),'secs')

    print("**************** " + "Check Performance Improvement using Fask Primary Key search" + " ****************")
    print("\n\n")


    #
    # result = csv_tbl.find_by_primary_key_fast(key_fields, field_list)
    #
    # print("CSV Table Test3 (Find By Primary Key Fast): Result = ", json.dumps(result, indent=2))


# Call Test Function
t_find_primary_key_fast()
