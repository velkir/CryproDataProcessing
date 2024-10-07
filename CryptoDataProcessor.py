import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import ccxt
from playwright.sync_api import sync_playwright
import numpy as np
import logging
from queue import Queue
from threading import Thread

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
        # engine = create_engine(connection_string, echo=True)
        engine = create_engine(connection_string, echo=False)

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
                # request = requests.get(self.binance_assets_url)
                request_data = self._makeRequestAssets()
                assetsDfAllColumns = pd.DataFrame(request_data["data"])
                assetsDfAllColumns.rename(columns={"name": "assetName", "cmcUniqueId": "cmcId", "tags": "tag"}, inplace=True)
                if assetHistory:
                    assetsDfAllColumns["timestamp"]=pd.Timestamp.now(tz="utc")
                    return assetsDfAllColumns[
                        ["assetName", "timestamp", "cmcId", "marketcap", "circulatingSupply", "maxSupply", "totalSupply"]]
                else:
                    return assetsDfAllColumns[["assetName", "cmcId", "tag"]]

            else:
                raise NotImplementedError("Добавлен только Binance")

        except Exception as e:
            print(f"An error occurred: {e}")

    def _makeRequestAssets(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # Запускаем браузер в фоновом режиме
            context = browser.new_context()
            page = context.new_page()
            # Переходим на сайт, чтобы получить куки
            page.goto('https://www.binance.com')
            # Дожидаемся прохождения проверки Cloudflare
            page.wait_for_load_state('networkidle')
            # Выполняем запрос и получаем ответ
            response = page.request.get(self.binance_assets_url)
            data = response.json()
            browser.close()
            return data

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
        return self.dataProcessor._select_pandas_query(query)

class AssetGroup:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor

    def addAssetGroup(self, assetGroupName):
        query = f"INSERT INTO AssetGroup (assetGroupName) VALUES ('{assetGroupName}');"
        return self.dataProcessor._basic_query(query=query)

    def deleteAssetGroup(self, assetGroupName):
        query = f"DELETE FROM AssetGroup WHERE assetGroupName = '{assetGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def renameAssetGroup(self, oldAssetGroupName, newAssetGroupName):
        query = f"UPDATE AssetGroup SET assetGroupName = '{newAssetGroupName}' WHERE assetGroupName = '{oldAssetGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def getAllAssetGroups(self):
        query = f"SELECT * FROM AssetGroup;"
        return self.dataProcessor._select_pandas_query(query=query)

    def addAssetsToAssetGroup(self, assetGroupName, assetsList):
        assetGroupId = self._getAssetGroupId(assetGroupName)
        addedAssetsCount = 0
        for asset in assetsList:
            query = f"INSERT INTO AssetGroupLinkTable (assetGroupId, assetName) VALUES ({assetGroupId}, '{asset}');"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                addedAssetsCount += success
        return addedAssetsCount

    def deleteAssetsFromAssetGroup(self, assetGroupName, assetsList):
        assetGroupId = self._getAssetGroupId(assetGroupName)
        deletedAssetsCount = 0
        for asset in assetsList:
            query = f"DELETE FROM AssetGroupLinkTable WHERE (assetGroupId={assetGroupId} AND assetName='{asset}');"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                deletedAssetsCount += success
        return deletedAssetsCount

    def getAssetsFromAssetGroup(self, assetGroupName):
        assetGroupId = self._getAssetGroupId(assetGroupName)
        query = f"SELECT assetName FROM assetGroupLinkTable WHERE assetGroupId = {assetGroupId};"
        return self.dataProcessor._select_pandas_query(query=query)

    def _getAssetGroupId(self, assetGroupName):
        query = f"SELECT assetGroupId FROM AssetGroup WHERE assetGroupName = '{assetGroupName}';"
        return self.dataProcessor._basic_query(query=query, queryType="select")[0][0]

class AssetHistory:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor

    def addAssetsHistoryRecords(self):
        #перепроверить после добавления vpn
        df = self.dataProcessor.asset._getAssetsFromExchange(assetHistory=True)
        return self.dataProcessor._modify_query_pandas(df, "AssetHistoryRecords")

    def getAssetHistoryRecords(self, assetName, fromDatetime=None):
        base_query = f"SELECT * FROM AssetHistory WHERE assetName = '{assetName}'"
        if fromDatetime:
            params = f" AND Timestamp >= '{fromDatetime}';"
        else:
           params = ";"
        query = base_query + params
        result = self.dataProcessor._select_pandas_query(query=query)
        if len(result) > 0:
            return result
        else:
            return None

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
        return self.dataProcessor._select_pandas_query(query)

    def getSymbolId(self, symbol, exchangeName="Binance", spot=True):
        if spot:
            isFutures = 0
        else:
            isFutures = 1
        query = f"SELECT symbolId FROM Ticker WHERE symbol='{symbol}' AND exchangeName='{exchangeName}' AND isFutures={isFutures};"
        return self.dataProcessor._basic_query(query, queryType="select")

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
            # отфильтровываю фьючи c экспирацией и обратные фьючи (где маржа - актив)
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

class TickerGroup:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor

    def addTickerGroup(self, tickerGroupName):
        query = f"INSERT INTO TickerGroup (tickerGroupName) VALUES '{tickerGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def deleteTickerGroup(self, tickerGroupName):
        query = f"DELETE FROM TickerGroup WHERE tickerGroupName = '{tickerGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def renameTickerGroup(self, oldTickerGroupName, newTickerGroupName):
        query = f"UPDATE TickerGroup SET tickerGroupName = '{newTickerGroupName}' WHERE assetGroupName = '{oldTickerGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def getAllTickerGroups(self):
        query = f"SELECT * FROM TickerGroup;"
        return self.dataProcessor._select_pandas_query(query=query)

    def addTickersToTickerGroup(self, tickerGroupName, symbolIds):
        tickerGroupId = self._getTickerGroupId(tickerGroupName)
        addedTickersCount = 0
        for symbolId in symbolIds:
            query = f"INSERT INTO TickerGroupLinkTable (tickerGroupId, symbolId) VALUES ({tickerGroupId}, {symbolId});"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                addedTickersCount += success
        return addedTickersCount

    def deleteTickersToTickerGroup(self, tickerGroupName, symbolIds):
        tickerGroupId = self._getTickerGroupId(tickerGroupName)
        deletedTickersCount = 0
        for symbolId in symbolIds:
            query = f"DELETE FROM TickerGroupLinkTable WHERE (tickerGroupId = {tickerGroupId} AND symbolId={symbolId});"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                deletedTickersCount += success
        return deletedTickersCount

    def getTickersFromTickerGroup(self, tickerGroupName):
        tickerGroupId = self._getTickerGroupId(tickerGroupName)
        query = f"SELECT symbolId FROM TickerGroupLinkTable WHERE tickerGroupId={tickerGroupId};"
        return self.dataProcessor._select_pandas_query(query=query)

    def _getTickerGroupId(self, tickerGroupName):
        query = f"SELECT tickerGroupId FROM TickerGroup WHERE tickerGroupName = '{tickerGroupName}';"
        return self.dataProcessor._basic_query(query=query, queryType="select")[0][0]

class CSVMerger:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.basicPath = os.getcwd()
        self.priceOHLCVSpotFolder = "data/spot/klines"
        self.priceOHLCVUmFuturesFolder = "data/futures/um/klines"
        self.fundingRatesFolder = "data/futures/um/fundingRate"
        self.metricsFolder = "data/futures/um/metrics"

    def mergePriceOHLCV(self, spotTickers=None, UMFuturesTickers=None, timeframe="1m", fromDateTime=None):
        spotPath = os.path.join(self.basicPath, self.priceOHLCVSpotFolder)
        umFuturesPath = os.path.join(self.basicPath, self.priceOHLCVUmFuturesFolder)
        queue = Queue()
        if spotTickers:
            downloadedSpotTickers = os.listdir(spotPath)
            [queue.put((spotTicker, spotPath, True)) for spotTicker in spotTickers if spotTicker in downloadedSpotTickers]

        if UMFuturesTickers:
            downloadedFuturesTickers = os.listdir(umFuturesPath)
            [queue.put((umFuturesTicker, umFuturesPath, False)) for umFuturesTicker in UMFuturesTickers if umFuturesTicker in downloadedFuturesTickers]

        numThreads = 30
        for i in range(numThreads):
            worker = Thread(target=self._merge1PriceOHLCV,
                            args=(queue,
                                  fromDateTime,
                                  timeframe))
            worker.start()
            queue.join()

    def _merge1PriceOHLCV(self, queue, fromDateTime=None, timeframe="1m"):
        while queue.not_empty:
            try:
                ticker, tickerPath, isSpot = queue.get_nowait()
                logger.info(f"Processing ticker: {ticker}, tickerPath: {tickerPath}")
                try:
                    symbolId = self._getSymbolId(symbol=ticker, isSpot=isSpot)
                    logger.debug(f"Obtained symbolId: {symbolId} for ticker: {ticker}")
                    tickerPath = os.path.join(tickerPath, ticker, timeframe)
                    tickerFiles = [file for file in os.listdir(tickerPath) if not file.startswith(".")]
                    logger.debug(f"Found {len(tickerFiles)} files for ticker: {ticker}")
                    dfTicker = pd.DataFrame(columns=[
                        "symbolId",
                        "timeframe",
                        "timestamp",
                        "open",
                        "high",
                        "low",
                        "close",
                        "baseAssetVolume",
                        "quoteAssetVolume",
                        "takerBuyBaseAssetVolume",
                        "takerBuyQuoteAssetVolume",
                        "tradesCount"
                    ])
                    for tickerFile in tickerFiles:
                        tickerFilePath = os.path.join(tickerPath, tickerFile)
                        logger.debug(f"Reading file: {tickerFilePath}")
                        dfTickerPartialData = pd.read_csv(
                            tickerFilePath,
                            names=[
                                "timestamp",
                                "open",
                                "high",
                                "low",
                                "close",
                                "baseAssetVolume",
                                "quoteAssetVolume",
                                "tradesCount",
                                "takerBuyBaseAssetVolume",
                                "takerBuyQuoteAssetVolume"
                            ],
                            usecols=[0, 1, 2, 3, 4, 5, 7, 8, 9, 10])
                        #в части файлов первая строка заголовок. В этом случае она обрезается
                        firstRow = dfTickerPartialData.loc[0, :].values.flatten().tolist()
                        if all([True for value in firstRow if isinstance(value, str)]):
                            dfTickerPartialData = dfTickerPartialData.iloc[1:, :]

                        dfTickerPartialData["symbolId"] = symbolId
                        dfTickerPartialData["timeframe"] = timeframe
                        dfTickerPartialData = dfTickerPartialData.iloc[:, [
                                                                              10, 11, 0, 1, 2, 3, 4, 5, 6, 9, 8, 7
                                                                          ]]
                        dfTicker = pd.concat([dfTicker, dfTickerPartialData], ignore_index=True)


                    dfTicker['timestamp'] = (dfTicker['timestamp'].astype(np.int64) / 1000).astype(np.int64)
                    dfTicker['timestamp'] = pd.to_datetime(dfTicker['timestamp'], unit="s")
                    dfTicker['baseAssetVolume'] = dfTicker['baseAssetVolume'].astype(np.float64).round().astype(np.int64)
                    dfTicker['quoteAssetVolume'] = dfTicker['quoteAssetVolume'].astype(np.float64).round().astype(np.int64)
                    dfTicker['takerBuyBaseAssetVolume'] = dfTicker['takerBuyBaseAssetVolume'].astype(np.float64).round().astype(np.int64)
                    dfTicker['takerBuyQuoteAssetVolume'] = dfTicker['takerBuyQuoteAssetVolume'].astype(np.float64).round().astype(np.int64)

                    result = self.dataProcessor._modify_query_pandas(dfTicker, "PriceOHLCV")
                    logger.info(f"Data processed and stored for ticker: {ticker}")
                except Exception as e:
                    logger.exception(f"Error while entering data into the database for ticker: {ticker}")
            except:
                logger.info("Queue is empty or an error occurred, worker shuts down")
                break

    def _getSymbolId(self, symbol, isSpot):
        return self.dataProcessor.ticker.getSymbolId(symbol=symbol, spot=isSpot)[0][0]

    def mergeFundingRates(self, UMFuturesTickers, fromDateTime=None):
        fundingRateFullPath = os.path.join(self.basicPath, self.fundingRatesFolder)
        queue = Queue()

        downloadedFuturesTickers = os.listdir(fundingRateFullPath)
        [queue.put(umFuturesTicker) for umFuturesTicker in UMFuturesTickers if umFuturesTicker in downloadedFuturesTickers]

        numThreads = 10
        for i in range(numThreads):
            worker = Thread(target=self._merge1FundingRate,
                            args=(queue,
                                  fundingRateFullPath,
                                  fromDateTime))
            worker.start()
            queue.join()

    def _merge1FundingRate(self, queue, fundingRateFullPath, fromDateTime):
        while queue.not_empty:
            try:
                ticker = queue.get_nowait()
                logger.info(f"Processing fundingRate for ticker: {ticker}")
                try:
                    symbolId = self._getSymbolId(symbol=ticker, isSpot=0)
                    logger.debug(f"Obtained symbolId: {symbolId} for ticker (futures): {ticker}")
                    tickerPath = os.path.join(fundingRateFullPath, ticker)
                    tickerFiles = [file for file in os.listdir(tickerPath) if not file.startswith(".")]
                    logger.debug(f"Found {len(tickerFiles)} files for ticker: {ticker}")
                    dfTicker = pd.DataFrame(columns=[
                        "symbolId",
                        "timestamp",
                        "fundingRate",
                        "fundingIntervalHours"
                    ])
                    for tickerFile in tickerFiles:
                        tickerFilePath = os.path.join(tickerPath, tickerFile)
                        logger.debug(f"Reading file: {tickerFilePath}")
                        dfTickerPartialData = pd.read_csv(
                            tickerFilePath,
                            names=[
                                "timestamp",
                                "fundingIntervalHours",
                                "fundingRate",
                            ])
                        # в части файлов первая строка заголовок. В этом случае она обрезается
                        firstRow = dfTickerPartialData.loc[0, :].values.flatten().tolist()
                        if all([True for value in firstRow if isinstance(value, str)]):
                            dfTickerPartialData = dfTickerPartialData.iloc[1:, :]

                        dfTickerPartialData["symbolId"] = symbolId
                        dfTickerPartialData = dfTickerPartialData.iloc[:, [
                                                                              3, 0, 2, 1
                                                                          ]]
                        dfTicker = pd.concat([dfTicker, dfTickerPartialData], ignore_index=True)

                    dfTicker['timestamp'] = (dfTicker['timestamp'].astype(np.int64) / 1000).astype(np.int64)
                    dfTicker['timestamp'] = pd.to_datetime(dfTicker['timestamp'], unit="s")

                    result = self.dataProcessor._modify_query_pandas(dfTicker, "FundingRate")
                    logger.info(f"Data processed and stored for ticker: {ticker}")
                except Exception as e:
                    logger.exception(f"Error while entering data into the database for ticker: {ticker}")
            except:
                logger.info("Queue is empty or an error occurred, worker shuts down")
                break

    def mergeMetrics(self, UMFuturesTickers, fromDateTime=None):
        metricsFullPath = os.path.join(self.basicPath, self.metricsFolder)
        queue = Queue()

        downloadedFuturesTickers = os.listdir(metricsFullPath)
        [queue.put(umFuturesTicker) for umFuturesTicker in UMFuturesTickers if umFuturesTicker in downloadedFuturesTickers]

        numThreads = 10
        for i in range(numThreads):
            worker = Thread(target=self._merge1Metrics,
                            args=(queue,
                                  metricsFullPath,
                                  fromDateTime))
            worker.start()
            queue.join()

    def _merge1Metrics(self, queue, metricsFullPath, fromDateTime, timeframe="5m"):
        while queue.not_empty:
            try:
                ticker = queue.get_nowait()
                logger.info(f"Processing metrics for ticker: {ticker}")
                try:
                    symbolId = self._getSymbolId(symbol=ticker, isSpot=0)
                    logger.debug(f"Obtained symbolId: {symbolId} for ticker (futures): {ticker}")
                    tickerPath = os.path.join(metricsFullPath, ticker)
                    tickerFiles = [file for file in os.listdir(tickerPath) if not file.startswith(".")]
                    logger.debug(f"Found {len(tickerFiles)} files for ticker: {ticker}")

                    dfOpenInterestTicker = pd.DataFrame(columns=[
                        "symbolId",
                        "timeframe",
                        "timestamp",
                        "openInterestUsd",
                        "openInterestAsset"
                    ])

                    dfLongShortRatio = pd.DataFrame(columns=[
                        "symbolId",
                        "timeframe",
                        "timestamp",
                        "topTradersAccountsRatio",
                        "topTradersPositionsRatio",
                        "takerVolumeRatio"
                    ])

                    for tickerFile in tickerFiles:
                        tickerFilePath = os.path.join(tickerPath, tickerFile)
                        logger.debug(f"Reading file: {tickerFilePath}")
                        dfTickerPartialData = pd.read_csv(
                            tickerFilePath,
                            names=[
                                "timestamp",
                                "symbol",
                                "openInterestAsset",
                                "openInterestUsd",
                                "topTradersAccountsRatio",
                                "topTradersPositionsRatio",
                                "temp",
                                "takerVolumeRatio"
                            ],
                            index_col=False)
                        # в части файлов первая строка заголовок. В этом случае она обрезается
                        firstRow = dfTickerPartialData.loc[0, :].values.flatten().tolist()
                        if all([True for value in firstRow if isinstance(value, str)]):
                            dfTickerPartialData = dfTickerPartialData.iloc[1:, :]

                        dfTickerPartialData["symbolId"] = symbolId
                        dfTickerPartialData["timeframe"] = timeframe
                        dfTickerPartialData.drop(columns=["symbol", "temp"], inplace=True)

                        dfOpenInterestTickerPartial = dfTickerPartialData.iloc[:, [
                                                                              6, 7, 0, 2, 1
                                                                          ]]
                        dfLongShortRatioPartial = dfTickerPartialData.iloc[:, [
                                                                              6, 7, 0, 3, 4, 5
                                                                          ]]
                        dfOpenInterestTicker = pd.concat([dfOpenInterestTicker, dfOpenInterestTickerPartial], ignore_index=True)
                        dfLongShortRatio = pd.concat([dfLongShortRatio, dfLongShortRatioPartial], ignore_index=True)

                        dfOpenInterestTicker['openInterestAsset'] = dfOpenInterestTicker['openInterestAsset'].astype(
                            np.float64).round().astype(
                            np.int64)
                        dfOpenInterestTicker['openInterestUsd'] = dfOpenInterestTicker['openInterestUsd'].astype(
                            np.float64).round().astype(
                            np.int64)

                    resultOpenInterest = self.dataProcessor._modify_query_pandas(dfOpenInterestTicker, "OpenInterest")
                    resultLongShortRatio = self.dataProcessor._modify_query_pandas(dfLongShortRatio, "LongShortRatio")
                    logger.info(f"Data processed and stored for ticker: {ticker}")
                except Exception as e:
                    logger.exception(f"Error while entering data into the database for ticker: {ticker}")
            except:
                logger.info("Queue is empty or an error occurred, worker shuts down")
                break

def loadTickersFromFile(path):
    with open(path, "r") as f:
        tickers = f.readlines()
        final_tickers = []
        for ticker in tickers:
            final_tickers.append(ticker.strip("'\n"))
        return final_tickers

spotTickers = loadTickersFromFile("spotTickers.txt")
umFuturesTickers = loadTickersFromFile("umFuturesTickers.txt")

pd.set_option("display.max_rows", 150)
pd.set_option("display.max_columns", 150)
pd.set_option("max_colwidth", 1000)
dataProcessor = CryptoDataProcessor("test")

# Настройка логгера
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
#
# # Создаем обработчик для вывода в консоль
# console_handler = logging.StreamHandler(sys.stdout)
# console_handler.setLevel(logging.DEBUG)
#
# # Создаем форматтер и добавляем его к обработчику
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# console_handler.setFormatter(formatter)
#
# # Добавляем обработчик к логгеру
# logger.addHandler(console_handler)


# dataProcessor.asset.updateAssets()
# dataProcessor.ticker.updateTickers()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spotTickersFile = open("spotTickers.txt", "r")
spotTickersList = spotTickersFile.readlines()
spotTickersList = [ticker.strip("'\n") for ticker in spotTickersList]


csvMerger = CSVMerger(dataProcessor=dataProcessor)
# csvMerger.mergePriceOHLCV(["BTCUSDT", "ETHUSDT"], timeframe="1h")
# csvMerger.mergePriceOHLCV(spotTickers=["BTCUSDT", "ETHUSDT", "DOGEUSDT"], UMFuturesTickers=["BTCUSDT", "ETHUSDT", "DOGEUSDT"], timeframe="1h")
# csvMerger.mergeFundingRates(UMFuturesTickers=["BTCUSDT", "ETHUSDT", "DOGEUSDT"])
csvMerger.mergeMetrics(UMFuturesTickers=["BTCUSDT", "ETHUSDT"])


# csvMerger.mergePriceOHLCV(spotTickersList, timeframe="1h")


#AssetGroup
# dataProcessor.assetGroup.addAssetGroup("Shitcoins")
# dataProcessor.assetGroup.addAssetGroup("L1")
# dataProcessor.assetGroup.addAssetGroup("L2")
# print(dataProcessor.assetGroup.getAllAssetGroups())
# dataProcessor.assetGroup.deleteAssetGroup("L1")
# dataProcessor.assetGroup.renameAssetGroup("L2", "L3")
# print(dataProcessor.assetGroup.getAllAssetGroups())
# dataProcessor.assetGroup.addAssetsToAssetGroup("Shitcoins", ["DOGE", "SHIB"])
# dataProcessor.assetGroup.deleteAssetsFromAssetGroup("Shitcoins", ["BTC", "ETH"])
# print(dataProcessor.assetGroup.getAssetsFromAssetGroup("Shitcoins"))
# print(dataProcessor.assetHistory.getAssetHistoryRecords("BTC"))

#TickerGroup
# print(dataProcessor.tickerGroup.getAllTickerGroups())
# print(dataProcessor.tickerGroup.getTickersFromTickerGroup("l1"))

