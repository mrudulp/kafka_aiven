import pytest

from dbworker import DBWorker
from cpu_stats_producer import CPUStatsProducer

# table_name = 'testTbl'

@pytest.fixture
def create_table():
    worker = DBWorker("test_db.ini")
    table_name = "testtbl"

    #Cleanup
    worker.ddl_query(f"DROP TABLE IF EXISTS {table_name}")
    # worker.close()

    # Test
    create_testtbl_table_qry = f'''
        CREATE TABLE {table_name} (
            Category        CHAR(50),
            SubCategory     CHAR(50),
            Value           REAL
        );
        '''
    worker.ddl_query(create_testtbl_table_qry)
    worker.close()

@pytest.fixture
def produce_messages_single():
    msg_cnt = 1
    producer = CPUStatsProducer("producer.ini")
    producer.produce_messages(msg_cnt)