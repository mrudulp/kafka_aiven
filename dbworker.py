from config import Config
import psycopg2
import json

class DBWorker:
    def __init__(self, *args, **kwargs):
        """
        Initialises DBWorker by loading settings from ini and opening a db connection

        Parameters
        ----------
        arg[0]: Name of the database's ini file
        """
        ini_file = args[0]
        self._config = Config(ini_file)
        uri = self._config.get_setting("db", "uri")
        self._db_conn = psycopg2.connect(uri)

    def __execute_query(self, query):
        """
        Executes Query and returns cursor

        Parameters
        ----------
        query: Query String
        """
        cursor = self._db_conn.cursor()
        cursor.execute(query)
        return cursor

    def create_table(self, create_table_qry):
        """
        Creates Table

        Parameters
        ----------
        create_table_qry: Query String for Table creation
        """
        cursor = self.__execute_query(create_table_qry)
        self._db_conn.commit()

    def insert_values(self, insert_qry):
        """
        Inserts values in the table

        Parameters
        ----------
        insert_qry: Query string for inserting values
        """
        cursor = self.__execute_query(insert_qry)
        self._db_conn.commit()

    def fetchAll(self, query):
        """
        Fetches all the records returned by the Query

        Parameters
        ----------
        query: Query String for fetching Records
        """

        cursor = self.__execute_query(query)
        rows = cursor.fetchall()
        return rows

    def close(self):
        """
        Closes open databse connection
        """
        self._db_conn.close()

if __name__ == '__main__':
    pass