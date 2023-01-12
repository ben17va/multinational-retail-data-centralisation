import pandas as pd
from database_utils import DatabaseConnector
class DataExtractor:
    def __init__(self, connector):
        self.connector = connector

    def read_data(self, query):
        # Get the engine from the connector
        engine = connector.init_db_engine()
        # Use the pandas read_sql function to execute the query and load the results into a DataFrame
        df = pd.read_sql(query, engine)
        return df

    def read_rds_table(self, legacy_users):
        # Get the engine from the connector
        engine = connector.init_db_engine()
        # Use the pandas read_sql function to load the table into a DataFrame
        df = pd.read_sql(f'SELECT * FROM {legacy_users}', engine)
        return df

connector = DatabaseConnector()
extractor = DataExtractor(connector)

# Get a list of all tables in the database
tables = connector.list_db_tables()
# Filter the list to get the table containing user data
user_data_tables = [table for table in tables if 'legacy_users' in table]
# Extract the first table containing user data
if user_data_tables:
    df = extractor.read_rds_table(user_data_tables[0])
else:
    df = pd.DataFrame()
    
df.head()
print (df) # Outputs a DataFrame containing the user data
