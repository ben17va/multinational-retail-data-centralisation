


             # Convert non-date columns to numeric datatype
        non_date_columns = df.select_dtypes(exclude=['object']).columns.tolist()
        df[non_date_columns] = df[non_date_columns].apply(pd.to_numeric, errors='coerce')

         # Remove rows that have wrong information
        df = df.dropna(subset=['user_id', 'name', 'email'])

         join_date = df.select_dtypes(['object']).columns.tolist()
        df[join_date] = df[join_date].apply(pd.to_datetime, errors='coerce')
        date_of_birth = df.select_dtypes(['object']).columns.tolist()
        df[date_of_birth] = df[date_of_birth].apply(pd.to_datetime, errors='coerce')

        def __init__(self, df_clean, legacy_users):
        self.df_clean = df_clean
        self.legacy_users = legacy_users




        tables = connector.list_db_tables()



def upload_to_db(df_clean, legacy_users):
        engine = connector.init_db_engine()
        df_clean = cleaner.clean_user_data(df_clean)
        df_clean.to_sql(legacy_users, engine, if_exists='replace')



    
cleaner = DataClean()  
connector = DatabaseConnector(cleaner)
uploader = DatabaseConnector(df_clean, 'dim_users')
uploader.upload_to_db() 


 def upload_to_db(df_clean, legacy_users):
        engine = connector.init_db_engine()
        df_clean = data_cleaning.DataClean(df_clean)
        df_clean.to_sql(legacy_users, engine, if_exists='replace')


         def __init__(self, df_clean, legacy_users):
        self.cleaner = data_cleaning.DataClean(df_clean)
        self.legacy_user = legacy_users
        

        def get_cleaned_data(self):
        return self.df_clean

         def __init__(self):
        self.df_clean = cleaner.clean_user_data(df)


            def upload_to_db(self):
        engine = self.init_db_engine()
        df_clean = .clean_user_data()
        df_clean.to_sql(self.table_name,engine, if_exists='replace') 


       import yaml
import sqlalchemy
import psycopg2 
from sqlalchemy import inspect
from sqlalchemy import create_engine
class DatabaseConnector:
    def __init__(self):
        self.database_name = 'Sales_Data'
        self.table_name = 'dim_users'

    def read_db_creds(self):
        with open('db_creds.yaml','r' ) as f:
            data_loaded = yaml.safe_load(f)
            return data_loaded


    def init_db_engine(self, Sales_Data = 'Sales_Data'): 
        self.database_name = Sales_Data
        creds = self.read_db_creds() 
        conn_str = conn_str = f'postgresql://{creds["RDS_USER"]}:{creds["RDS_PASSWORD"]}@{creds["RDS_HOST"]}:{creds["RDS_PORT"]}/{self.database_name}'
        engine = sqlalchemy.create_engine(conn_str)
        return engine

    def upload_to_db(self, df_clean):
        # Get the engine
        engine = self.init_db_engine('Sales_Data')
        # upload data
        df_clean.to_sql(self.table_name, engine, if_exists='replace')



        cleaner = DataClean()
df_clean = cleaner.clean_user_data(df)  
print(df_clean)
db_connector = DatabaseConnector()
db_connector.upload_to_db(newdf_clean, connection_type='local')

 valid_providers = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
 'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover','VISA 19 digit'
 'VISA 16 digit', 'VISA 13 digit']
        newdf = newdf[newdf['card_provider'].isin(valid_providers)]



        extractor = DataExtractor(connector)
newdf = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    
newdf.info()
print (newdf) # Outputs a DataFrame containing the user data
def retrieve_stores_data(self, store_number):
        store_data = []
        response = requests.get(f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}", headers=self.header)
        if response.status_code == 200:
            store_data = response.json()
            store_data_df = pd.DataFrame(store_data)
            return store_data_df
        else:
            raise Exception("Failed to retrieve store data. Response code: " + str(response.status_code))
newdf_copy = newdf.copy()
cleaner = DataClean()
newdf_clean = cleaner.clean_card_data(newdf_copy)
print(newdf_clean)
db_connector = DatabaseConnector()
db_connector.upload_to_db(newdf_clean, connection_type='local')


header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
extractor = DataExtractor(connector)
number_of_stores = extractor.list_number_of_stores(number_stores_endpoint, header) 
print(number_of_stores)

retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
store_data_df = extractor.retrieve_stores_data(retrieve_store_endpoint)
print(store_data_df)