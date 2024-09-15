import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import requests
import json

class CryptoDataProcessor:
    def __init__(self, dbName):
        self._engine = self._initialize_db_connection(dbName)
        self.exchange = Exchange(self)
        self.exchangeBalanceHistory = ExchangeBalanceHistory(self)
        self.asset = Asset(self)

    def _initialize_db_connection(self, dbName):
        connection_string = f"mysql+mysqlconnector://user:1234@localhost:3306/{dbName}"
        engine = create_engine(connection_string, echo=True)
        return engine

    def _select_query(self, query, params=None, parse_dates=None, columns=None, chunksize=None):
        try:
            with self._engine.connect() as con:
                return pd.read_sql(sql=query,
                                   con=con,
                                   params=params,
                                   parse_dates=parse_dates,
                                   columns=columns,
                                   chunksize=chunksize)
        except Exception as e:
            print(f"An error occurred: {e}")

    def _modify_query(self, query, params=None):
        try:
            with self._engine.connect() as con:
                result = con.execute(text(query), params)
                con.commit()
                return result.rowcount
        except Exception as e:
                print(f"An error occurred: {e}")

    def _modify_query_pandas(self, df, table, if_exists='append', index=False, method="multi"):
        try:
            return df.to_sql(name=table, con=self._engine, if_exists=if_exists, index=index, method=method)
        except Exception as e:
                print(f"An error occurred: {e}")

class Exchange:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor

    def add_exchange(self, exchangeName):
        query = f"INSERT INTO Exchange (exchangeName) VALUES ('{exchangeName}');"
        return self.dataProcessor._modify_query(query)

    def delete_exchange(self, exchangeName):
        query = f"DELETE FROM Exchange WHERE exchangeName='{exchangeName}';"
        return self.dataProcessor._modify_query(query)

    def getExchanges(self, params=None, parse_dates=None, columns=None, chunksize=None):
        query = f"SELECT * FROM Exchange;"
        return self.dataProcessor._select_query(query=query,
                                                params=params,
                                                parse_dates=parse_dates,
                                                columns=columns,
                                                chunksize=chunksize)

class ExchangeBalanceHistory:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor

    def updateExchangeBalance(self, exchangeName):
        #Логика запроса к coinglass  https://docs.coinglass.com/reference/exchange-balance-list
        #Логика обработки данных в pd.df и добавление в БД
        pass

    def getLastExchangeBalanceHistoryRecord(self, exchangeName):
        query = f"SELECT exchangeName, timestamp, balance FROM ExchangeBalanceHistory WHERE exchangeName='{exchangeName}' ORDER BY timestamp DESC LIMIT 1;"
        return self.dataProcessor._select_query(query=query)

class Asset:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.binance_assets_url = "https://www.binance.com/bapi/apex/v1/friendly/apex/marketing/complianceSymbolList"

    def getAssetsFromDB(self):
        query = f"SELECT * FROM Asset ORDER BY cmcId"
        return self.dataProcessor._select_query(query=query)

    def getAssetsFromExchange(self, exchangeName, assetHistory=False):
        try:
            if exchangeName == "Binance":
                request = requests.get(self.binance_assets_url)
                assetsDfAllColumns = pd.DataFrame(json.loads(request.content)["data"])
                assetsDfAllColumns.rename(columns={"name": "assetName", "cmcUniqueId": "cmcId"}, inplace=True)
                if assetHistory:
                    return assetsDfAllColumns[["assetName", "cmcId", "marketcap" ,"circulatingSupply", "maxSupply", "totalSupply"]]
                else:
                    return assetsDfAllColumns[["assetName", "cmcId"]]
            else:
                raise NotImplementedError("Добавлен только Binance")
        except Exception as e:
            print(f"An error occurred: {e}")


    def updateAssets(self, exchangeName, DBAssets=None, exchangeAssets=None):
        if exchangeAssets is None:
            if exchangeName == "Binance":
                exchangeAssets = self.getAssetsFromExchange(exchangeName)
            else:
                raise NotImplementedError("Добавлен только Binance")

        if DBAssets is None:
            DBAssets = self.getAssetsFromDB()

        missingAssets = exchangeAssets[~exchangeAssets["cmcId"].isin(DBAssets["cmcId"])]
        return self.dataProcessor._modify_query_pandas(missingAssets, "Asset", if_exists='append', index=False, method="multi")

dataProcessor = CryptoDataProcessor("test")
print(dataProcessor.asset.updateAssets("Binance"))
print()
# print(dataProcessor.exchange.add_exchange("Kraken"))
# df = dataProcessor.exchange.getExchanges()
# print(df.head(10))
# print(dataProcessor.exchange.delete_exchange("Bae"))