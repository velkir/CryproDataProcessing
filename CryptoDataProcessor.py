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
                request = requests.get(self.binance_assets_url)
                assetsDfAllColumns = pd.DataFrame(json.loads(request.content)["data"])
                assetsDfAllColumns.rename(columns={"name": "assetName", "cmcUniqueId": "cmcId", "tags": "tag"}, inplace=True)
                if assetHistory:
                    return assetsDfAllColumns[
                        ["assetName", "cmcId", "marketcap", "circulatingSupply", "maxSupply", "totalSupply"]]
                else:
                    return assetsDfAllColumns[["assetName", "cmcId", "tag"]]

            else:
                raise NotImplementedError("Добавлен только Binance")

        except Exception as e:
            print(f"An error occurred: {e}")


    def updateAssets(self, exchangeName="Binance", DBAssets=None, exchangeAssets=None):
        if exchangeAssets is None:
            if exchangeName == "Binance":
                exchangeAssets = self._getAssetsFromExchange(exchangeName)
            else:
                raise NotImplementedError("Добавлен только Binance")
        if DBAssets is None:
            DBAssets = self._getAssetsFromDB()
        newAssets = exchangeAssets[~exchangeAssets["cmcId"].isin(DBAssets["cmcId"])]
        newAssets_without_tags = newAssets.drop(columns="tag")
        if not newAssets.empty:
            assets_affected_rows = self.dataProcessor._modify_query_pandas(newAssets_without_tags, "Asset",
                                                                           if_exists='append', index=False, method="multi")
            df_tags = newAssets[["assetName", "tag"]].explode("tag")
            df_tags.dropna(inplace=True)
            tags_affected_rows = self.dataProcessor.assetTag.addTags(df_tags)
            return assets_affected_rows, tags_affected_rows


class AssetTag:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor

    def addTags(self, df_tags):
        return self.dataProcessor._modify_query_pandas(df_tags, "AssetTag", if_exists='append', index=False,
                                                       method="multi")

    def getTags(self, assetsList=None):
        query = f"SELECT * FROM AssetTag"
        params = ";"
        if assetsList:
            params = f" WHERE {''.join(['symbol = ' + '\'' + str(asset) + '\' OR ' for asset in assetsList])}"
            params = params[:-4] + ";"
        query += params
        return self.dataProcessor._select_query(query)


class Ticker:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor

    def getTickersFromDB(self, symbolsList=None, params=None):
        query = f"SELECT * FROM Ticker"
        params = ";"
        if symbolsList:
            params = f" WHERE {''.join(['symbol = ' + '\'' + str(symbol) + '\' OR ' for symbol in symbolsList])}"
            params = params[:-4] + ";"
        query += params
        return self.dataProcessor._select_query(query)

    def updateTickers(self):
        df_exchange_tickers = self.getTickersFromExchange()
        df_db_tickers = self.getTickersFromDB()
        # дроп symbolId, чтобы пустое значение не пошло в БД
        df_db_tickers.drop(columns=["symbolId"],
                           inplace=True)
        merged_df = pd.merge(df_exchange_tickers,
                             df_db_tickers,
                             on=['symbol', 'exchangeName', 'isFutures'],
                             how='outer',
                             indicator=True,
                             suffixes=('', '_y'))
        newTickers = merged_df[merged_df['_merge'] != 'both'].drop(columns=['_merge'])
        columns_to_drop = [column for column in newTickers.columns if column.endswith("_y")]
        newTickers.drop(columns=columns_to_drop,
                        inplace=True)
        return self.addTickers(newTickers)

    def getTickersFromExchange(self, exchangeName="Binance", whiteListedQuoteAssets=True):
        if exchangeName == "Binance":
            df_all_tickers = pd.DataFrame(self.dataProcessor.ccxtBinance.fetchMarkets())
            # отфильтровываю не бесконечные фьючи и обратные фьючи (где маржа - актив)
            df_spot_futures = df_all_tickers[((df_all_tickers["type"] == "spot") | (
                    (df_all_tickers["type"] == "swap") & df_all_tickers["linear"] == True)) & (
                                                         df_all_tickers["active"] == True)]
            if whiteListedQuoteAssets:
                with open("quoteAssetWhitelist.txt", "r") as f:
                    quoteAssets = f.read().split("\n")
                    df_spot_futures = df_spot_futures[df_spot_futures["quote"].isin(quoteAssets)]
            # Т.к. биржа не отдает прямо время листинга тикера, то его буду получать при первом добавлении PriceOHLCV
            df_spot_futures["listingDatetime"] = None
            # дропаю лишние колонки
            df_spot_futures = df_spot_futures[["id", "active", "swap", "margin", "listingDatetime", "base", "quote"]]
            # вручную задаю exchangeId
            df_spot_futures["exchangeName"] = "Binance"
            # переименовывания для соответствия БД
            df_spot_futures.rename(columns={"id": "symbol",
                                            "active": "isActive",
                                            "swap": "isFutures",
                                            "margin": "hasMargin",
                                            "base": "baseAssetName",
                                            "quote": "quoteAssetName"}, inplace=True)
            return df_spot_futures
        else:
            raise NotImplementedError("Добавлен только Binance")

    def addTickers(self, dfNewTickers):
        return self.dataProcessor._modify_query_pandas(dfNewTickers, "Ticker", if_exists='append', index=False,
                                                       method="multi", chunksize=1)


pd.set_option("display.max_rows", 150)
pd.set_option("display.max_columns", 150)
pd.set_option("max_colwidth", 1000)
dataProcessor = CryptoDataProcessor("test")
dataProcessor.asset.updateAssets()
dataProcessor.ticker.updateTickers()
print()