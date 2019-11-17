import pytest
from time import sleep
from dbworker import DBWorker

def test_ddl(create_table):
    '''
    Testing Creation of Table api works fine
    Note: Test Fixture does the cleanup/creation before tests
    '''
    worker = DBWorker("test_db.ini")
    table_name = "testtbl"

    # rows = worker.fetchAll(f"select * from {table_name}")
    # we dont create Table separately as it will be created by TestFixture
    # verification
    existingTableQuery = f"select * from information_schema.tables where table_name='{table_name}'"
    rows = worker.fetchAll(existingTableQuery)
    result = len(rows)
    assert (result == 1)

def test_dml(create_table):
    '''
    Test data manipulation
    Note: Test Fixture does the cleanup/creation before tests
    '''
    worker = DBWorker("test_db.ini")
    table_name = "testtbl"
    category = "cpu"
    subcategory = "cpucnt"
    value = 2
    worker.dml_query(f"INSERT INTO {table_name} VALUES('{category}', '{subcategory}', {value})")
    rows = worker.fetchAll(f"select * from {table_name}")
    result = len(rows)
    assert (result == 1)

# # https://stackoverflow.com/questions/11536764/how-to-fix-attempted-relative-import-in-non-package-even-with-init-py/27876800#27876800
if __name__ == '__main__':
    if __package__ is None or __package__ is '':
        import sys
        from os import path
        sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
        from dbworker import DBWorker
    else:
        from dbworker import DBWorker
    # test_createTable()
    # test_insert_values("test")