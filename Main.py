import pyodbc 
import pandas as pd
import sqlalchemy
import urllib

class DataExtractor:
    def __init__(self, filePath):
        self.filePath = filePath

    def extract_data(self):
        # header=0 will start from the first row of data, ignore the header
        histdata = pd.read_csv(self.filePath, header=0, names=["Date", "OpenPrice", "High", "Low", "ClosePrice","AdjustedClose","Volume"])
        return histdata

class DataTransformer:
    def adjusted_cost(self, x):
        if(x > 500):
            return 'High'
        else:
            return 'Low'

    def transform(self, histdata):
        histdata['State'] = histdata['AdjustedClose'].apply(lambda x: self.adjusted_cost(x))
        return histdata

class DataLoader:
    def load(self, refinedhistdata):
        server = '<Server>' 
        database = '<DB>' 
        username = 'User' 
        password = 'PWD' 

        params = urllib.quote_plus("DRIVER={ODBC Driver 13 for SQL Server};SERVER="+ server +";DATABASE="+ database+";UID="+ username+";PWD="+ password+"")
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        # write the DataFrame to a table in the sql database, if data exists it will replace
        # index=False will make sure it will not create an index column
        refinedhistdata.to_sql("SNP500", engine, if_exists='replace', index=False)

class Controller:
    def __init__(self, filePath):
        self.filePath = filePath

    def run(self):
        extractor = DataExtractor(self.filePath)
        histdata = extractor.extract_data()
        transformer = DataTransformer()
        refinedhistdata = transformer.transform(histdata)
        loader = DataLoader()
        loader.load(refinedhistdata)

def main():
    controller = Controller('c:\\data\\snp500.csv')
    controller.run()

if __name__ == "__main__": main()