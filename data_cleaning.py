import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import awsdf 
class DataClean:
     
    def clean_user_data(self,df): 
        #drop any null values
        df = df.dropna()
        # Convert date columns to datetime datatype
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%Y-%m-%d', errors='coerce')
        df = df[df['date_of_birth'].notnull()]
        df['join_date'] = pd.to_datetime(df['join_date'], format='%Y-%m-%d', errors='coerce')
        df = df[df['join_date'].notnull()]
        
        #checking for duplicates
        duplicated_rows = df.duplicated().sum()
        if duplicated_rows > 0:
            print(f'{duplicated_rows} duplicate rows found')

        return df

    def clean_card_data(self, newdf):
        newdf = newdf.dropna()
        
        newdf['card_number'] = newdf['card_number'].replace('[^0-9]', '', regex=True)
        newdf = newdf[newdf['card_number'].notnull()]
        newdf = newdf[newdf['card_number'] != '']

        newdf = newdf[newdf['card_provider'].notnull()]

        newdf['date_payment_confirmed'] = pd.to_datetime(newdf['date_payment_confirmed'], format='%Y-%m-%d', errors='coerce')
        newdf = newdf[newdf['date_payment_confirmed'].notnull()]

        newdf['expiry_date'] = pd.to_datetime(newdf['expiry_date'], format='%m/%y', errors= 'coerce') 
        newdf = newdf[newdf['expiry_date'].notnull()]
        newdf['expiry_date'] = newdf['expiry_date'].dt.strftime('%m/%y')


        duplicated_rows = newdf.duplicated().sum()
        if duplicated_rows > 0:
            print(f'{duplicated_rows} duplicate rows found')

        return newdf

    def clean_store_data(self,store_data_df):
        store_data_df = store_data_df.drop(["lat"], axis=1)
        
        store_data_df["opening_date"] = pd.to_datetime(store_data_df["opening_date"], format='%Y-%m-%d', errors= 'coerce')
        store_data_df= store_data_df[store_data_df["opening_date"].notna()]  
        store_data_df["staff_numbers"] = pd.to_numeric(store_data_df["staff_numbers"], errors='coerce')
        store_data_df = store_data_df[store_data_df["staff_numbers"].notnull()]
        store_data_df["staff_numbers"] = store_data_df["staff_numbers"].astype(int)
        store_data_df["longitude"] = pd.to_numeric(store_data_df["longitude"], errors='coerce')
        store_data_df = store_data_df[store_data_df["longitude"].notnull()]
        store_data_df["latitude"] = pd.to_numeric(store_data_df["latitude"], errors='coerce')
        store_data_df = store_data_df[store_data_df["latitude"].notnull()]

        valid_countrycodes = ["GB","DE","US"]
        store_data_df = store_data_df.loc[store_data_df["country_code"].isin(valid_countrycodes)]
        store_data_df = store_data_df[store_data_df["country_code"].notnull()] 

        store_data_df["continent"] = store_data_df["continent"].replace(to_replace=["eeEurope"], value="Europe", regex=True)
        store_data_df["continent"] = store_data_df["continent"].replace(to_replace=["eeAmerica"], value="America", regex=True)
        valid_continent = ["Europe","America"]
        store_data_df = store_data_df.loc[store_data_df["continent"].isin(valid_continent)]
        store_data_df = store_data_df[store_data_df["continent"].notnull()]  

        validstores = ["Local","Super Store", "Mall Kiosk", "Outlet"]
        store_data_df = store_data_df.loc[store_data_df["store_type"].isin(validstores)]
        store_data_df = store_data_df[store_data_df["store_type"].notnull()] 

        store_data_df = store_data_df[store_data_df["store_code"].notnull()]
        store_data_df = store_data_df.loc[store_data_df["locality"] != "3VHFDNP8ET"]

        store_data_df["address"] = store_data_df["address"].str.split("\n").apply(lambda x : ' '.join(x))
        store_data_df = store_data_df.dropna()

        duplicated_rows = store_data_df.duplicated().sum()
        if duplicated_rows > 0:
            print(f'{duplicated_rows} duplicate rows found')

        return store_data_df

    def convert_product_weights(self, awsdf):
        # extract weight unit from weight column
        awsdf.loc[:, 'weight_unit'] = awsdf['weight'].str.extract(r'([a-zA-Z]+)$')
        # remove unit from weight column
        awsdf.loc[:, 'weight'] = awsdf['weight'].str.replace(r'[a-zA-Z]', '')
        # convert weights column to float
        awsdf['weight'] = pd.to_numeric(awsdf['weight'], errors='coerce')
        # drop rows with invalid weight values
        awsdf.dropna(subset=['weight'], inplace=True)
        # convert weights in ml to kg
        awsdf.loc[(awsdf['weight_unit'] == 'ml'), 'weight'] = awsdf['weight'] / 1000000
        # convert weights in g to kg
        awsdf.loc[(awsdf['weight_unit'] == 'g'), 'weight'] = awsdf['weight'] / 1000
        # Keep the rows that have a 'kg' value as it is
        awsdf.loc[(awsdf['weight_unit'] == 'kg'), 'weight'] = awsdf['weight']
        # drop rows with invalid weight unit values
        awsdf = awsdf[awsdf.weight_unit.isin(["g", "kg","ml"])]

        return awsdf

    def clean_products_data(self, awsdf):
        awsdf["date_added"] = pd.to_datetime(awsdf["date_added"], format='%Y-%m-%d', errors= 'coerce')
        awsdf= awsdf[awsdf["date_added"].notna()]
        awsdf["EAN"] = pd.to_numeric(awsdf["EAN"], errors= 'coerce')
        awsdf = awsdf[awsdf["EAN"].notnull()] 
        awsdf = awsdf[awsdf["uuid"].notnull()] 
        expected_categories = ["homeware", "toys-and-games", "food-and-drink", "pets", "sports-and-leisure", "health-and-beauty", "diy"]
        awsdf = awsdf[awsdf["category"].isin(expected_categories)]
        awsdf = awsdf[awsdf["category"].notnull()] 
        stockcheck = ["Still_avaliable", "Removed"]
        awsdf = awsdf[awsdf["removed"].isin(stockcheck)]
        awsdf = awsdf[awsdf["category"].notnull()] 
        awsdf = awsdf.rename(columns={'removed':'Availability'})
        awsdf['Availability'].replace(to_replace =['Still_avaliable'], value ='Available',inplace=True)
        awsdf['product_price'] = pd.to_numeric(awsdf['product_price'].str.strip('£'), errors='coerce')
        awsdf.dropna(subset=['product_price'], inplace=True)
        awsdf['product_price'] = '£'+awsdf['product_price'].astype(str)

        duplicated_rows = awsdf.duplicated().sum()
        if duplicated_rows > 0:
            print(f'{duplicated_rows} duplicate rows found')

        awsdf.reset_index(drop=True, inplace=True)
        awsdf.drop(columns=['Unnamed: 0'], axis=1, inplace=True)
        awsdf.drop(columns='weight_unit', axis=1, inplace=True)
        awsdf = awsdf.dropna()

        return awsdf

cleaner = DataClean()
awsdf_copy = awsdf.copy()
cleaner.convert_product_weights(awsdf_copy)
cleanawsdf = cleaner.clean_products_data(awsdf_copy)





        








