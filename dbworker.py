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

        uri = self._config.get_setting('db', 'uri')
        host = self._config.get_setting('db', 'host')
        port = self._config.get_setting("db", "port")
        dbname = self._config.get_setting("db", "database")
        user = self._config.get_setting("db", "user")
        password = self._config.get_setting("db", "password")

        self._db_conn = psycopg2.connect(user=user, password=password, dbname=dbname, host=host, port=port)

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

    def ddl_query(self, ddl_qry):
        """
        DDL Query handles(Create, Drop, Alters) Table

        Parameters
        ----------
        ddl_qry: Query String for Table creation
        """
        cursor = self.__execute_query(ddl_qry)
        self._db_conn.commit()

    def dml_query(self, dml_qry):
        """
        Inserts values in the table

        Parameters
        ----------
        dml_qry: Query string for inserting values
        """
        cursor = self.__execute_query(dml_qry)
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