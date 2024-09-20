import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import requests
import json
import ccxt
import datetime


class CryptoDataProcessor:
    def __init__(self, dbName):
        self._engine = self._initialize_db_connection(dbName)
        self.ccxtBinance = ccxt.binance()
        self.exchange = Exchange(self)
        self.exchangeBalanceHistory = ExchangeBalanceHistory(self)
        self.asset = Asset(self)
        self.assetTag = AssetTag(self)
        self.assetGroup = AssetGroup(self)
        self.assetHistory = AssetHistory(self)
        self.ticker = Ticker(self)
        self.tickerGroup = TickerGroup(self)

    def _initialize_db_connection(self, dbName):
        connection_string = f"mysql+mysqlconnector://user:1234@localhost:3306/{dbName}"
        engine = create_engine(connection_string, echo=True)
        return engine

    def _basic_query(self, query, queryType="modify", params=None):
        try:
            with self._engine.connect() as con:
                result = con.execute(text(query), params)
                con.commit()
                if queryType == "modify":
                    return result.rowcount
                elif queryType == "select":
                    return result.fetchall()
        except Exception as e:
                print(f"An error occurred: {e}")

    def _select_pandas_query(self, query, params=None, parse_dates=None, columns=None, chunksize=None):
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

    def _modify_query_pandas(self, df, table, if_exists='append', index=False, method="multi", chunksize=500):
        try:
            return df.to_sql(name=table, con=self._engine, if_exists=if_exists, index=index, method=method, chunksize=chunksize)
        except Exception as e:
                print(f"An error occurred: {e}")

class Exchange:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor

    def add_exchange(self, exchangeName):
        query = f"INSERT INTO Exchange (exchangeName) VALUES ('{exchangeName}');"
        return self.dataProcessor._basic_query(query)

    def delete_exchange(self, exchangeName):
        query = f"DELETE FROM Exchange WHERE exchangeName='{exchangeName}';"
        return self.dataProcessor._basic_query(query)

    def getExchange(self, exchangeName):
        query = f"SELECT * FROM Exchange WHERE exchangeName = '{exchangeName}';"
        result = self.dataProcessor._select_pandas_query(query=query)
        if len(result) > 0:
            return result
        else:
            return None

    def getAllExchanges(self, params=None, parse_dates=None, columns=None, chunksize=None):
        query = f"SELECT * FROM Exchange;"
        return self.dataProcessor._select_pandas_query(query=query,
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

    def getExchangeBalanceHistoryRecords(self, exchangeName, fromDatetime=None):
        base_query = f"SELECT * FROM ExchangeBalanceHistory WHERE exchangeName='{exchangeName}'"
        params = ""
        closing_part = " ORDER BY timestamp;"
        if fromDatetime is not None:
            params = f" AND timestamp >= '{fromDatetime}'"
        query = base_query + params + closing_part
        return self.dataProcessor._select_pandas_query(query=query)

    def getLastExchangeBalanceHistoryRecord(self, exchangeName):
        query = f"SELECT balance FROM ExchangeBalanceHistory WHERE exchangeName='{exchangeName}' ORDER BY timestamp DESC LIMIT 1;"
        result = self.dataProcessor._basic_query(query=query, queryType="select")
        if result:
            return result[0][0]
        else:
            return None

class Asset:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.binance_assets_url = "https://www.binance.com/bapi/apex/v1/friendly/apex/marketing/complianceSymbolList"

    def _getAssetsFromDB(self):
        query = f"SELECT * FROM Asset ORDER BY cmcId"
        return self.dataProcessor._select_pandas_query(query=query)

    def _getAssetsFromExchange(self, exchangeName="Binance", assetHistory=False):
        try:
            if exchangeName == "Binance":
                request = req