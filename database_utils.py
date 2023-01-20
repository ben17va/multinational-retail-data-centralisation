import yaml
import sqlalchemy 
from sqlalchemy import inspect
from sqlalchemy import create_engine
class DatabaseConnector:
    def __init__(self):
        self.database_name = 'Sales_Data'
        self.table_name = 'dim_product'

    def read_db_creds(self):
        with open('db_creds.yaml','r' ) as f:
            data_loaded = yaml.safe_load(f)
            return data_loaded


    def init_db_engine(self): 
        creds = self.read_db_creds() 
        conn_str = conn_str = 'postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}'.format(**creds)
        engine = sqlalchemy.create_engine(conn_str)
        return engine


    def connect_local(self):
        newconn_str = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user='postgres',
            password='pereira31',
            host='localhost',
            port='5432',
            database='Sales_Data'
        )
        engine = create_engine(newconn_str)
        return engine

    
    def upload_to_db(self, cleanawsdf, connection_type = 'local'):
        if connection_type == 'local':
            engine = self.connect_local()
        else:
            engine = self.init_db_engine()()
    
        cleanawsdf.to_sql(self.table_name, engine, if_exists='replace',index=False)


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










