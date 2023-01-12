import pandas as pd
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

from data_extraction import df     
cleaner = DataClean()
df_clean = cleaner.clean_user_data(df)  
print(df_clean)

