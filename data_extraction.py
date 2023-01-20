import tabula 
import pandas as pd
import requests
import boto3
from io import StringIO
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

    def read_rds_table(self, orders_table):
        engine = connector.init_db_engine()
        # Use the pandas read_sql function to load the table into a DataFrame
        df = pd.read_sql(f'SELECT * FROM {orders_table}', engine)
        return df


    def retrieve_pdf_data(self,link):
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        pdf_data = tabula.read_pdf(link, pages='all', multiple_tables=True) 
        newdf = pd.concat(pdf_data)
        return newdf 


    header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    def list_number_of_stores(self, number_stores_endpoint, header):
        response = requests.get(number_stores_endpoint, headers = header)
        if response.status_code == 200:
            number_stores = response.json()['number_stores']
            return number_stores
        else:
            raise Exception("Failed to retrieve number of stores. Response code: " + str(response.status_code))

    def retrieve_stores_data(self, retrieve_store_endpoint):
        header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        store_data = []
        store_number = 1
        while True:
            response = requests.get(retrieve_store_endpoint.format(store_number = store_number), headers = header)
            if response.status_code == 200:
                store_data.append(response.json())
                store_number += 1
            else:
                break
        store_data_df = pd.DataFrame(store_data)
        return store_data_df


    def extract_from_s3(self, s3_address):
        s3_address = 's3://data-handling-public/products.csv'
        s3 = boto3.client('s3')
        bucket_name = s3_address.split("/")[2]
        file_name = s3_address.split("/")[-1]
        s3.download_file(bucket_name, file_name, Filename = 'products.csv')
        awsdf = pd.read_csv(file_name) 
        return awsdf



connector = DatabaseConnector()
extractor = DataExtractor(connector)

# Get a list of all tables in the database
tables = connector.list_db_tables()
# Filter the list to get the table containing user data
orders_data_tables = [table for table in tables if 'orders_table' in table]
# Extract the first table containing user data
if orders_data_tables:
    df = extractor.read_rds_table(orders_data_tables[0])
else:
    df = pd.DataFrame()

print(df['card_number'].value_counts())   


 
