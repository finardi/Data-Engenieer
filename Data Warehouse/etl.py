import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

class ETLPipeline():
    """
    A class used to prepare the ETL pipeline, i.e. load and execute the insertions into tables
    
    . . .
    
    Methods
    -------
    _load_staging_tables() 
        execute the staging tables events and songs
    insert_tables()
        execute the insert into tables songplays, users, songs, artists and time
    """
    def __init__(self, conn, cur):
        """
        Parameters
        ----------
        conn : object
            postgres connection
        cur : object
            postgres connection cursor
        """
        self.conn = conn
        self.cur  = cur
    
    def _load_staging_tables(self):
        """Performs the staging tables in RedShit"""
        
        for query in copy_table_queries:
            self.cur.execute(query)
            self.conn.commit()

    def insert_tables(self):
        """Performs the inserts into tables"""
        
        self._load_staging_tables()
        for query in insert_table_queries:
            self.cur.execute(query)
            self.conn.commit()
        self.conn.close()


# Config file to make connection into RedShit        
config = configparser.ConfigParser()
config.read('config.cfg')

HOST     = config.get("SET_CONFIG","HOST")
DB       = config.get("SET_CONFIG","DB")
USER     = config.get("SET_CONFIG","USER")
PASSWORD = config.get("SET_CONFIG","PASSWORD")
PORT     = config.get("SET_CONFIG","PORT")

conn = psycopg2.connect(f"host={HOST} dbname={DB} user={USER} password={PASSWORD} port={PORT}")
cur  = conn.cursor()

# Execute the ETL pipeline
MakeETL = ETLPipeline(conn, cur)
MakeETL.insert_tables()
    
    
    