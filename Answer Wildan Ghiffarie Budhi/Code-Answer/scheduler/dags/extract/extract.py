from os import name
import typing
from zipfile import ZipFile
from pandas.core.frame import DataFrame
from requests import get 
from io import BytesIO, StringIO
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable

class ExtractFileFrowGit():

    TABLE_ORDER = ["REGION", "NATION", "PART", "SUPPLIER", "PARTSUPP", "CUSTOMER", "ORDERS", "LINEITEM"]

    def run(self):
        zipped_data = self.download_data("https://raw.githubusercontent.com/rezaprimasatya/DTO-DE-JUN-MID/main/data.zip")
        unzipped_data = self.unzip_data(zipped_data)
        data_dataframes = self.data_to_df(unzipped_data)
        self.df_to_datalake(data_dataframes)

    def download_data(self, url: str, chunk_size: int = 1024) -> BytesIO:

        print("Downloading data from ->", url)

        data = BytesIO()
        r = get(url, stream=True)

        for chunk in r.iter_content(chunk_size=chunk_size):
            data.write(chunk)
        
        r.close()
            
        return data

    def unzip_data(self, zipped_data: BytesIO) -> typing.Dict[str, StringIO]:

        print("Unzipping Data")

        result = {}
        z = ZipFile(zipped_data)

        for fileinfo in z.infolist():
            data = z.read(fileinfo)
            data = str(data, "utf-8")
            data = StringIO(data)
            result[fileinfo.filename.replace(".tbl", "").upper()] = data
        
        z.close()
        
        return result
    
    def data_to_df(self, unzipped_data: typing.Dict[str, StringIO]) -> typing.Dict[str, pd.DataFrame]:

        print("Transform Data to Pandas Dataframe")

        result = {}

        for name, data in unzipped_data.items():
            df = pd.read_table(data, delimiter="|", header=None)
            df = df.drop(df.columns[len(df.columns)-1], axis=1)
            result[name] = df
        
        return result
    
    def df_to_datalake(self, data_dataframes: typing.Dict[str, pd.DataFrame]):

        print("Load data to datalake")

        datalake_conn = create_engine(
            "mysql://{user}:{pasword}@{host}:{port}/datalake".format(
                user=Variable.get("DWH_USER"),
                pasword=Variable.get("DWH_PASSWORD"),
                host=Variable.get("DWH_HOST"),
                port=Variable.get("DWH_PORT")
            )
        )

        get_table_col_names_template = """
        SELECT 
            COLUMN_NAME 
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_NAME = '{table_name}'
            AND TABLE_SCHEMA = 'datalake'
        ORDER BY 
            ORDINAL_POSITION ASC 
        """

        for table_name in self.TABLE_ORDER:

            print("Load table :", table_name)

            df = data_dataframes[table_name]

            columns = pd.read_sql(get_table_col_names_template.format(table_name=table_name), datalake_conn)
            columns = columns["COLUMN_NAME"].to_list()[:-1]
            df.columns = columns

            df.to_sql(name=table_name, con=datalake_conn, if_exists='append', index= False)

def extract_data_from_git():
    pipeline = ExtractFileFrowGit()
    pipeline.run()

