from src.CSVCatalog import ColumnDefinition, CSVCatalog, IndexDefinition, TableDefinition
import json
import csv
import time

people_pk      = ['playerID']
batting_pk     = ['playerID', 'teamID', 'yearID', 'stint']
appearances_pk = ['playerID', 'teamID', 'yearID']


def read_input_file(fn):
    rows = []
    columns = None
    with open(fn, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames
        # for inp in reader:
        #     rows.append(inp)
    column_def = []
    for col in columns:
        tmp = ColumnDefinition(col)
        column_def.append(tmp)

    return columns, column_def

#### Check the creation of medadata file.
def test1a():
    try:
        print('Check the creation of Metadata file; Table is People;')
        print('####################################################')

        tn = 'People'
        file_name='People.csv'
        columns, column_def = read_input_file("../data/"+file_name)
        index_def = []
        pk_index = IndexDefinition('PRIMARY_KEY','PRIMARY',people_pk)
        index_def.append(pk_index)

        catalog = CSVCatalog()
        table_def = catalog.create_table(table_name=tn, file_name=file_name,
                                      column_definitions=column_def, index_definitions=index_def)

        print('Table Name : ', tn)
        print('Columns : ', columns)
        print('Primary Key : \n', pk_index)
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test1 Exception: ", e)

#### Check the creation of medadata file.
def test1b():
    try:
        print('Check the creation of medadata file; Table is Appearances')
        print('####################################################')

        tn = 'Appearances'
        file_name='Appearances.csv'
        columns, column_def = read_input_file("../data/"+file_name)
        index_def = []
        pk_index = IndexDefinition('PRIMARY_KEY','PRIMARY',appearances_pk)
        index_def.append(pk_index)


        catalog = CSVCatalog()
        table_def = catalog.create_table(table_name=tn, file_name=file_name,
                                      column_definitions=column_def, index_definitions=index_def)

        print('Table Name : ', tn)
        print('Columns : ', columns)
        print('Primary Key : \n', pk_index)
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test1 Exception: ", e)

#### Check the creation of medadata file.
def test1c():
    try:
        print('Check the creation of medadata file; Table is Batting')
        print('####################################################')

        tn = 'Batting'
        file_name='Batting.csv'
        columns, column_def = read_input_file("../data/"+file_name)
        index_def = []
        pk_index = IndexDefinition('PRIMARY_KEY','PRIMARY',batting_pk)
        index_def.append(pk_index)


        catalog = CSVCatalog()
        table_def = catalog.create_table(table_name=tn, file_name=file_name,
                                      column_definitions=column_def, index_definitions=index_def)

        print('Table Name : ', tn)
        print('Columns : ', columns)
        print('Primary Key : \n', pk_index)
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test1 Exception: ", e)

#### Check the creation of medadata file.
def test1d():
    try:
        print('Check the creation of medadata file; Table is People_test, this is a file with sample records of People')
        print('####################################################')

        tn = 'People_test'
        file_name='People_test.csv'
        columns, column_def = read_input_file("../data/"+file_name)
        index_def = []
        pk_index = IndexDefinition('PRIMARY_KEY','PRIMARY',people_pk)
        index_def.append(pk_index)


        catalog = CSVCatalog()
        table_def = catalog.create_table(table_name=tn, file_name=file_name,
                                      column_definitions=column_def, index_definitions=index_def)

        print('Table Name : ', tn)
        print('Columns : ', columns)
        print('Primary Key : \n', pk_index)
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test1 Exception: ", e)


#### Load from the existing metadata file.
def test2():
    try:
        print('get_table method - Load from the existing metadata file created above. Note that the metadata files are stored as .metadata file.')
        print('####################################################')

        tn = 'People_test'

        catalog = CSVCatalog()
        table_def = catalog.get_table(table_name=tn)

        print('Table Name : ', tn)
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test2 Exception: ", e)

#### Dropping the table when the given table doesn't exist; Should throw an error.
def test3():
    try:
        print('Dropping the table when the given table do not exist; Should throw an error; Checking for People_test2 here.')
        print(' ####################################################')

        tn = 'People_test2'

        catalog = CSVCatalog()
        table_def = catalog.drop_table(table_name=tn)

        print('Table Name : ', tn)
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test3 Exception: ", e)

#### Dropping the existing table ; Try to load the table after it's dropped, should give an error.
def test4():
    try:
        print('Dropping the existing table ; Try to load the table after its dropped, should give an error.')
        print('####################################################')

        tn = 'People_test'

        catalog = CSVCatalog()
        table_def = catalog.drop_table(table_name=tn)
        table_def = catalog.get_table(table_name=tn)
        print('Table Name : ', tn)
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test4 Exception: ", e)

### Dropping an existing column;
def test5():
    try:
        print('Dropping an existing column; Notice that birthYear is dropped')
        print('####################################################')

        tn = 'People_test'

        catalog = CSVCatalog()
        table_def = catalog.get_table(table_name=tn)
        print('Table Name : ', tn)
        print('Table Metadata from the input file : \n', table_def)
        table_def.drop_column_definition('birthYear')
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test5 Exception: ", e)

### Dropping an  index; When index name (birthYear) doesn't exist
def test6a():
    try:
        print('Dropping an  index; When index name do not  exist')
        print('####################################################')

        tn = 'People_test'

        catalog = CSVCatalog()
        table_def = catalog.get_table(table_name=tn)
        print('Table Name : ', tn)
        table_def.drop_index('birthYear')
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test5 Exception: ", e)

### Dropping an existing index;
def test6b():
    try:
        print('Dropping an existing index - PRIMARY_KEY')
        print('####################################################')

        tn = 'People_test'

        catalog = CSVCatalog()
        table_def = catalog.get_table(table_name=tn)
        print('Table Name : ', tn)
        print('Table Metadata from the input file : \n', table_def)
        table_def.drop_index('PRIMARY_KEY')
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test6b Exception: ", e)

### Adding primary key;
def test7():
    try:
        print('Adding primary key;')
        print('####################################################')

        tn = 'People_test'

        catalog = CSVCatalog()
        table_def = catalog.get_table(table_name=tn)
        print('Table Name : ', tn)
        print('Table Metadata from the input file : \n', table_def)
        table_def.define_primary_key(['playerId'])
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test7 Exception: ", e)

### Adding an INDEX;
def test8():
    try:
        print('Adding an INDEX - NEW_INDEX;')
        print('####################################################')

        tn = 'People_test'

        catalog = CSVCatalog()
        table_def = catalog.get_table(table_name=tn)
        print('Table Name : ', tn)
        print('Table Metadata from the input file : \n', table_def)
        table_def.save_index_definition('NEW_INDEX',['nameFirst','nameLast'],'INDEX')
        print('Table Metadata from the output file : \n', table_def)

    except Exception as e:
        print("Test8 Exception: ", e)

### Read index column names;
def test9():
    try:
        print('Read index column names;')
        print('####################################################')

        tn = 'People_test'

        catalog = CSVCatalog()
        table_def = catalog.get_table(table_name=tn)
        print('Table Metadata :\n', table_def, '\n')
        cols = table_def.get_index_cols('NEW_INDEX')
        print('Column Names :', cols)

    except Exception as e:
        print("Test9 Exception: ", e)

test1a()
test1b()
test1c()
test1d()
test2()
test3()
test4()
test1d()
test5()
test6a()
test6b()
test7()
test8()
test9()