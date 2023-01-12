import yaml
import sqlalchemy
import psycopg2 
from sqlalchemy import inspect
from sqlalchemy import create_engine
class DatabaseConnector:
    def __init__(self):
        self.table_name = 'dim_users'

    def read_db_creds(self):
        with open('db_creds.yaml','r' ) as f:
            data_loaded = yaml.safe_load(f)
            return data_loaded


    def init_db_engine(self):
        creds = self.read_db_creds() 
        conn_str = 'postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}'.format(**creds)
        engine = sqlalchemy.create_engine(conn_str)
        return engine

    def upload_to_db(self):
        # import cleaned data
        from data_cleaning import df_clean
        # Get the engine
        engine = self.init_db_engine()
        # upload data
        df_clean.to_sql(self.table_name, engine, if_exists='replace')


    def list_db_tables(self):
        engine = self.init_db_engine()
        engine.connect()
        # Create an Inspector object
        inspector = inspect(engine)
        # Get a list of all tables in the database
        tables = inspector.get_table_names()
        return tables 


connector = DatabaseConnector()
tables = connector.list_db_tables()
print(tables)
connector = DatabaseConnector()
connector.upload_to_db()









