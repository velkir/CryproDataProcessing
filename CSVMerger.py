import os
import pandas as pd
import numpy as np
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class CSVMerger:
    def __init__(self, dataProcessor, logger):
        self.logger = logger
        self.logger.info("Initializing CSVMerger.")
        self.dataProcessor = dataProcessor
        self.basicPath = os.getcwd()
        self.shared_lock = threading.Lock()
        self.priceOHLCVSpotFolder = "data/spot/klines"
        self.priceOHLCVUmFuturesFolder = "data/futures/um/klines"
        self.fundingRatesFolder = "data/futures/um/fundingRate"
        self.metricsFolder = "data/futures/um/metrics"
        self.logger.info("CSVMerger initialized successfully.")

    def mergePriceOHLCV(self, spotTickers=None, umFuturesTickers=None, timeframe="1m", fromDateTime=None):
        spotPath = os.path.join(self.basicPath, self.priceOHLCVSpotFolder)
        umFuturesPath = os.path.join(self.basicPath, self.priceOHLCVUmFuturesFolder)
        tasks = []

        self.logger.info(f"Starting mergePriceOHLCV with timeframe: {timeframe} and fromDateTime: {fromDateTime}")

        if spotTickers:
            downloadedSpotTickers = os.listdir(spotPath)
            self.logger.info(f"Found {len(downloadedSpotTickers)} spot tickers in directory: {spotPath}")

            tasks.extend(
                [(spotTicker, spotPath, 1) for spotTicker in spotTickers if spotTicker in downloadedSpotTickers])

        if umFuturesTickers:
            downloadedFuturesTickers = os.listdir(umFuturesPath)
            self.logger.info(f"Found {len(downloadedFuturesTickers)} UM Futures tickers in directory: {umFuturesPath}")

            tasks.extend([(umFuturesTicker, umFuturesPath, 0) for umFuturesTicker in umFuturesTickers if
                          umFuturesTicker in downloadedFuturesTickers])

        numThreads = 30
        self.logger.info(f"Starting {numThreads} threads to process the tasks")

        with ThreadPoolExecutor(max_workers=numThreads) as executor:
            futures = {executor.submit(self._merge1PriceOHLCV, task, fromDateTime, timeframe): task for task in tasks}

            for future in as_completed(futures):
                task = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.exception(f"An error occurred processing task {task}: {e}")

        self.logger.info("Completed merging OHLCV data.")

    def _merge1PriceOHLCV(self, task, fromDateTime=None, timeframe="1m"):
        ticker, tickerPath, isSpot = task
        thread_name = threading.current_thread().name
        self.logger.info(f"Thread {thread_name}: Processing OHLCV for ticker: {ticker}, isSpot: {isSpot}")

        try:
            with self.shared_lock:
                symbolId = self._getSymbolId(symbol=ticker, spot=isSpot)
            self.logger.info(f"Thread {thread_name}: Obtained symbolId: {symbolId} for ticker: {ticker}")
            tickerPath = os.path.join(tickerPath, ticker, timeframe)
            tickerFiles = [file for file in os.listdir(tickerPath) if not file.startswith(".")]
            self.logger.info(f"Thread {thread_name}: Found {len(tickerFiles)} files for ticker: {ticker}")
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
                self.logger.info(f"Thread {thread_name}: Reading file: {tickerFilePath}")
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
                    usecols=[0, 1, 2, 3, 4, 5, 7, 8, 9, 10]
                )
                firstRow = dfTickerPartialData.loc[0, :].values.flatten().tolist()
                if all([isinstance(value, str) for value in firstRow]):
                    dfTickerPartialData = dfTickerPartialData.iloc[1:, :]

                dfTickerPartialData["symbolId"] = symbolId
                dfTickerPartialData["timeframe"] = timeframe
                dfTickerPartialData = dfTickerPartialData.iloc[:, [
                    10, 11, 0, 1, 2, 3, 4, 5, 6, 9, 8, 7
                ]]
                dfTicker = pd.concat([dfTicker, dfTickerPartialData], ignore_index=True)

            dfTicker['timestamp'] = pd.to_datetime(dfTicker['timestamp'].astype(np.int64) // 1000, unit="s")
            dfTicker['baseAssetVolume'] = dfTicker['baseAssetVolume'].astype(np.float64).round().astype(np.int64)
            dfTicker['quoteAssetVolume'] = dfTicker['quoteAssetVolume'].astype(np.float64).round().astype(np.int64)
            dfTicker['takerBuyBaseAssetVolume'] = dfTicker['takerBuyBaseAssetVolume'].astype(np.float64).round().astype(
                np.int64)
            dfTicker['takerBuyQuoteAssetVolume'] = dfTicker['takerBuyQuoteAssetVolume'].astype(
                np.float64).round().astype(np.int64)

            result = self.dataProcessor._modify_query_pandas(
                dfTicker,
                "PriceOHLCV"
            )
            self.logger.info(f"Thread {thread_name}: Data processed and stored for ticker: {ticker}, isSpot: {isSpot}")
        except Exception as e:
            self.logger.exception(f"Thread {thread_name}: Error while entering data into the database for ticker: {ticker}, isSpot: {isSpot}")

    def _getSymbolId(self, symbol, exchangeName="Binance", spot=True):
        self.logger.info("Fetching symbol ID for symbol: %s", symbol)
        if spot:
            isFutures = 0
        else:
            isFutures = 1
        query = f"SELECT symbolId FROM Ticker WHERE symbol='{symbol}' AND exchangeName='{exchangeName}' AND isFutures={isFutures};"
        result = self.dataProcessor._basic_query(query, queryType="select")
        if result:
            self.logger.info("Symbol ID found: %s for symbol: %s", result[0][0], symbol)
            return result[0][0]
        else:
            self.logger.info("Symbol ID not found for symbol: %s", symbol)
            return None

    def mergeFundingRates(self, UMFuturesTickers, fromDateTime=None):
        fundingRateFullPath = os.path.join(self.basicPath, self.fundingRatesFolder)
        tasks = []

        self.logger.info(f"Starting mergeFundingRates with fromDateTime: {fromDateTime}")

        downloadedFuturesTickers = os.listdir(fundingRateFullPath)
        self.logger.info(f"Found {len(downloadedFuturesTickers)} funding rate tickers in directory: {fundingRateFullPath}")

        tasks.extend([ticker for ticker in UMFuturesTickers if ticker in downloadedFuturesTickers])

        numThreads = 30
        self.logger.info(f"Starting {numThreads} threads to process the funding rates")

        with ThreadPoolExecutor(max_workers=numThreads) as executor:
            futures = {executor.submit(self._merge1FundingRate, ticker, fundingRateFullPath, fromDateTime): ticker for ticker in tasks}

            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.exception(f"An error occurred processing funding rates for ticker {ticker}: {e}")

        self.logger.info("Completed merging funding rates.")

    def _merge1FundingRate(self, ticker, fundingRateFullPath, fromDateTime):
        thread_name = threading.current_thread().name
        self.logger.info(f"Thread {thread_name}: Processing funding rate for ticker: {ticker}")

        try:
            with self.shared_lock:
                symbolId = self._getSymbolId(symbol=ticker, spot=0)
            self.logger.info(f"Thread {thread_name}: Obtained symbolId: {symbolId} for ticker (futures): {ticker}")
            tickerPath = os.path.join(fundingRateFullPath, ticker)
            tickerFiles = [file for file in os.listdir(tickerPath) if not file.startswith(".")]
            self.logger.info(f"Thread {thread_name}: Found {len(tickerFiles)} files for ticker: {ticker}")
            dfTicker = pd.DataFrame(columns=[
                "symbolId",
                "timestamp",
                "fundingRate",
                "fundingIntervalHours"
            ])
            for tickerFile in tickerFiles:
                tickerFilePath = os.path.join(tickerPath, tickerFile)
                self.logger.info(f"Thread {thread_name}: Reading file: {tickerFilePath}")
                dfTickerPartialData = pd.read_csv(
                    tickerFilePath,
                    names=[
                        "timestamp",
                        "fundingIntervalHours",
                        "fundingRate",
                    ])
                firstRow = dfTickerPartialData.loc[0, :].values.flatten().tolist()
                if all([isinstance(value, str) for value in firstRow]):
                    dfTickerPartialData = dfTickerPartialData.iloc[1:, :]

                dfTickerPartialData["symbolId"] = symbolId
                dfTickerPartialData = dfTickerPartialData.iloc[:, [
                    3, 0, 2, 1
                ]]
                dfTicker = pd.concat([dfTicker, dfTickerPartialData], ignore_index=True)

            dfTicker['timestamp'] = pd.to_datetime((dfTicker['timestamp'].astype(np.int64) / 1000).astype(np.int64), unit="s")

            result = self.dataProcessor._modify_query_pandas(
                dfTicker,
                "FundingRate"
            )
            self.logger.info(f"Thread {thread_name}: Data processed and stored for funding rates ticker: {ticker}")
        except Exception as e:
            self.logger.exception(f"Thread {thread_name}: Error while processing funding rates for ticker: {ticker}")

    def mergeMetrics(self, UMFuturesTickers, fromDateTime=None):
        metricsFullPath = os.path.join(self.basicPath, self.metricsFolder)
        tasks = []

        self.logger.info(f"Starting mergeMetrics with fromDateTime: {fromDateTime}")

        downloadedFuturesTickers = os.listdir(metricsFullPath)
        self.logger.info(f"Found {len(downloadedFuturesTickers)} metrics tickers in directory: {metricsFullPath}")

        tasks.extend([ticker for ticker in UMFuturesTickers if ticker in downloadedFuturesTickers])

        numThreads = 30
        self.logger.info(f"Starting {numThreads} threads to process the metrics")

        with ThreadPoolExecutor(max_workers=numThreads) as executor:
            futures = {executor.submit(self._merge1Metrics, ticker, metricsFullPath, fromDateTime): ticker for ticker in tasks}

            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.exception(f"An error occurred processing metrics for ticker {ticker}: {e}")

        self.logger.info("Completed merging metrics.")

    def _merge1Metrics(self, ticker, metricsFullPath, fromDateTime, timeframe="5m"):
        thread_name = threading.current_thread().name
        self.logger.info(f"Thread {thread_name}: Processing metrics for ticker: {ticker}")

        try:
            with self.shared_lock:
                symbolId = self._getSymbolId(symbol=ticker, spot=0)
            self.logger.info(f"Thread {thread_name}: Obtained symbolId: {symbolId} for ticker (futures): {ticker}")
            tickerPath = os.path.join(metricsFullPath, ticker)
            tickerFiles = [file for file in os.listdir(tickerPath) if not file.startswith(".")]
            self.logger.info(f"Thread {thread_name}: Found {len(tickerFiles)} files for ticker: {ticker}")

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
                self.logger.info(f"Thread {thread_name}: Reading file: {tickerFilePath}")
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
                firstRow = dfTickerPartialData.loc[0, :].values.flatten().tolist()
                if all([isinstance(value, str) for value in firstRow]):
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
                    np.float64).round().astype(np.int64)
                dfOpenInterestTicker['openInterestUsd'] = dfOpenInterestTicker['openInterestUsd'].astype(
                    np.float64).round().astype(np.int64)

            resultOpenInterest = self.dataProcessor._modify_query_pandas(
                dfOpenInterestTicker,
                "OpenInterest"
            )
            resultLongShortRatio = self.dataProcessor._modify_query_pandas(
                dfLongShortRatio,
                "LongShortRatio"
            )
            self.logger.info(f"Thread {thread_name}: Data processed and stored for metrics ticker: {ticker}")
        except Exception as e:
            self.logger.exception(f"Thread {thread_name}: Error while processing metrics for ticker: {ticker}")

def loadTickersFromFile(path):
    with open(path, "r") as f:
        tickers = f.readlines()
        final_tickers = [ticker.strip("'\n") for ticker in tickers]
        return final_tickers