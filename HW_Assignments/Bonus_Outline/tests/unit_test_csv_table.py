from src.CSVCatalog import ColumnDefinition, CSVCatalog, IndexDefinition, TableDefinition
import src.CSVCatalog as CSVCatalog
import src.CSVTable as CSVTable
import csv
import json
import time


people_pk      = ['playerID']

def read_input_file(fn):
    rows = []
    columns = None
    with open(fn, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames

    column_def = []
    for col in columns:
        tmp = ColumnDefinition(col)
        column_def.append(tmp)

    return columns, column_def


tn = 'People_join_test'
print('Table Name :', tn)
people_join_test = CSVTable.CSVTable(tn)


tn = 'Appearances'
print('Table Name :', tn)
appearances = CSVTable.CSVTable(tn)

tn = 'People'
print('Table Name :', tn)
people = CSVTable.CSVTable(tn)

tn = 'Batting'
print('Table Name :', tn)
batting = CSVTable.CSVTable(tn)

#### Check the creation of medadata file.
def test0():
    try:
        print('####################################################')

        tn = 'People_join_test'
        file_name='People_join_test.csv'
        columns, column_def = read_input_file("../data/"+file_name)
        index_def = []
        pk_index = IndexDefinition('PRIMARY_KEY','PRIMARY',people_pk)
        index_def.append(pk_index)

        catalog = CSVCatalog.CSVCatalog()
        table_def = catalog.create_table(table_name=tn, file_name=file_name,
                                      column_definitions=column_def, index_definitions=index_def)

        print('Table Name : ', tn)
        print('Columns : ', columns)
        print('Primary Key : \n', pk_index)

    except Exception as e:
        print("Test0 Exception: ", e)


##### Load People and Appearances and JOIN on playerID with OPTIMIZER for enhanced performance.

def test1():
    try:
        print('####################################################')
        print('Join my people_test_join table with Appearances table using smart join')
        print('####################################################')

        on_fields = ['playerID']
        where_template = {'playerID': 'aasedo01', 'teamID': 'BAL'}
        project_fields = ['playerID', 'nameFirst', 'nameLast', 'yearID','teamID', 'G_all']

        print('Join Fields: ', on_fields)
        print('Where template:', where_template)
        print('Projected fields:', project_fields)


        #########  WITH OPTIMIZER
        start_time = time.time()

        j = people_join_test.execute_smart_join(appearances, on_fields,
                    where_template,project_fields)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Elapsed_Time (With Optimizer) = ", elapsed_time)
        print("\n No. of rows in Output : ", len(j) ," \n Output rows = ", json.dumps(j, indent=2))

        print('####################################################')
    except Exception as e:
        print("test1 (Join Tables) Exception: ", e)

##### Load People and Appearances and JOIN on playerID with dumb_join
def test2():
    try:

        print('####################################################')
        print('Join my people_test_join table with Appearances table using dumb join')
        print('####################################################')

        on_fields = ['playerID']
        where_template = {'playerID': 'aasedo01', 'teamID': 'BAL'}
        project_fields = ['playerID', 'nameFirst', 'nameLast', 'yearID', 'teamID', 'G_all']

        print('Join Fields: ', on_fields)
        print('Where template:', where_template)
        print('Projected fields:', project_fields)

        start_time = time.time()

        j = people_join_test.dumb_join(appearances, on_fields, where_template,project_fields)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Elapsed_Time (Without Optimizer) = ", elapsed_time)
        print("\n No. of rows in Output : ", len(j) ," \n Output rows = ", json.dumps(j, indent=2))


    except Exception as e:
        print("test2 (Dumb Join) Exception: ", e)

##### Load People and Appearances and JOIN on playerID with OPTIMIZER for enhanced performance.

def test3():
    try:
        print('####################################################')
        print('Join People table with Appearances table using smart join')
        print('####################################################')

        on_fields = ['playerID']
        where_template = {'playerID': 'aasedo01', 'teamID': 'BAL'}
        project_fields = ['playerID', 'nameFirst', 'nameLast', 'yearID','teamID', 'G_all']

        print('Join Fields: ', on_fields)
        print('Where template:', where_template)
        print('Projected fields:', project_fields)


        #########  WITH OPTIMIZER
        start_time = time.time()

        j = people.execute_smart_join(appearances, on_fields, where_template,project_fields)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Elapsed_Time (With Optimizer) = ", elapsed_time)
        print("\n No. of rows in Output : ", len(j) ," \n Output rows = ", json.dumps(j, indent=2))

        print('####################################################')
    except Exception as e:
        print("test3 (Join Tables) Exception: ", e)

##### Load People and Appearances and JOIN on playerID with dumb_join
def test4():
    try:

        print('####################################################')
        print('Join  people table with Appearances table using dumb join')
        print('####################################################')

        on_fields = ['playerID']
        where_template = {'playerID': 'aasedo01', 'teamID': 'BAL'}
        project_fields = ['playerID', 'nameFirst', 'nameLast', 'yearID', 'teamID', 'G_all']

        print('Join Fields: ', on_fields)
        print('Where template:', where_template)
        print('Projected fields:', project_fields)

        start_time = time.time()

        j = people.dumb_join(appearances, on_fields, where_template,project_fields)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Elapsed_Time (Without Optimizer) = ", elapsed_time)
        print("\n No. of rows in Output : ", len(j) ," \n Output rows = ", json.dumps(j, indent=2))


    except Exception as e:
        print("test4 (Dumb Join) Exception: ", e)



def test5():
    try:
        print('####################################################')
        print('Join People table with Batting table using smart join')
        print('####################################################')

        on_fields = ['playerID']
        where_template = {'teamID': 'BOS', 'yearID': '1960', 'throws': 'L' }
        project_fields = ['playerID', 'nameLast', 'throws', 'teamID', 'yearID', 'AB','H', 'RBI']

        print('Join Fields: ', on_fields)
        print('Where template:', where_template)
        print('Projected fields:', project_fields)

        #########  WITH OPTIMIZER
        start_time = time.time()

        j = people.execute_smart_join(batting, on_fields, where_template, project_fields)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Elapsed_Time (With Optimizer) = ", elapsed_time)
        print("\n No. of rows in Output : ", len(j) ," \n Output rows = ", json.dumps(j, indent=2))

        print('####################################################')
    except Exception as e:
        print("test5 (Smart Join Tables) Exception: ", e)

##### Load People and Appearances and JOIN on playerID with dumb_join
def test6():
    try:

        print('####################################################')
        print('Join People table with Batting table using dumb join')
        print('####################################################')

        on_fields = ['playerID']
        where_template = {'teamID': 'BOS', 'yearID': '1960', 'throws': 'L' }
        project_fields = ['playerID', 'nameLast', 'throws', 'teamID', 'yearID', 'AB','H', 'RBI']

        print('Join Fields: ', on_fields)
        print('Where template:', where_template)
        print('Projected fields:', project_fields)

        start_time = time.time()

        j = people.dumb_join(batting, on_fields, where_template,project_fields)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print("Elapsed_Time (Without Optimizer) = ", elapsed_time)
        print("\n No. of rows in Output : ", len(j) ," \n Output rows = ", json.dumps(j, indent=2))


    except Exception as e:
        print("test6 (Dumb Join) Exception: ", e)

# test0()
# test1()
# test2()
test3()
test4()
test5()
test6()