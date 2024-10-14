from BinanceDataDumper import BinanceDataDumper
from CryptoDataProcessor import CryptoDataProcessor
from CSVMerger import CSVMerger
import pandas as pd
import warnings
import logging
import sys
import os
import shutil

class Controller:
    def __init__(self, database="test"):
        self.logger = self._initialize_logger()
        self.dbConnector = CryptoDataProcessor(dbName=database, logger=self.logger)
        self.binanceMerger = CSVMerger(dataProcessor=self.dbConnector, logger=self.logger)
        self.binanceDumper = BinanceDataDumper(logger=self.logger)
        self._set_pandas_options()

    def _initialize_logger(self):
        warnings.filterwarnings('ignore')
        # Configure logging to output to both console and file with timestamps
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Remove all existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Create handlers for console and file
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        file_handler = logging.FileHandler('controller.log', mode='a', encoding='utf-8')
        file_handler.setLevel(logging.INFO)

        # Create formatter with timestamp
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Set formatter for handlers
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        return logger

    def _set_pandas_options(self):
        pd.set_option("display.max_rows", 150)
        pd.set_option("display.max_columns", 150)
        pd.set_option("max_colwidth", 1000)

    def updateNonIndicators(self):
        try:
            self.dbConnector.asset.updateAssets()
        except Exception as e:
            self.logger.info(f"Failed to update Assets. Error: {e}")
        try:
            self.dbConnector.ticker.updateTickers()
        except Exception as e:
            self.logger.info(f"Failed to update Tickers. Error: {e}")

    def updateIndicators(self, indicators=["klines", "metrics", "fundingRate"], timeframes=["1m"], initialUpdate=False):
        try:
            spotTickers, futuresTickers = self._getSpotFuturesTickersFromDB()
            self._updateDumperAttrs(spotTickers, futuresTickers, indicators, timeframes=timeframes)
            try:
                # временное херовое решение
                if not initialUpdate:
                    lastRecordPriceOHLCV = self.dbConnector.indicators.getIndicatorRecords(
                        indicatorTable="PriceOHLCV",
                        symbol="BTCUSDT",
                        spot=True,
                        timeframe="1m",
                        lastRecord=True
                    )
                    lastRecordMetrics = self.dbConnector.indicators.getIndicatorRecords(
                        indicatorTable="OpenInterest",
                        symbol="BTCUSDT",
                        spot=False,
                        timeframe="1m",
                        lastRecord=True
                    )
                    lastRecordFundingRate = self.dbConnector.indicators.getIndicatorRecords(
                        indicatorTable="FundingRate",
                        symbol="BTCUSDT",
                        spot=False,
                        timeframe="1m",
                        lastRecord=True
                    )
                    fromDatePriceOHLCV = self.dbConnector.indicators._getNextDate(lastRecord=lastRecordPriceOHLCV)
                    fromDateMetrics = self.dbConnector.indicators._getNextDate(lastRecord=lastRecordMetrics)
                    fromDateFundingRate = self.dbConnector.indicators._getNextDate(lastRecord=lastRecordFundingRate)
                else:
                    fromDatePriceOHLCV = None
                    fromDateMetrics = None
                    fromDateFundingRate = None
                try:
                    self.binanceDumper.spotTickers = ["BTCUSDT"]
                    self.binanceDumper.umFuturesTickers = ["BTCUSDT"]
                    # self.binanceDumper.dataTypes = ["fundingRate"]
                    self.binanceDumper.dumpData(
                        dateStartPriceOHLCV=fromDatePriceOHLCV,
                        dateStartMetrics=fromDateMetrics,
                        dateStartFundingRate=fromDateFundingRate
                    )
                    try:
                        for timeframe in timeframes:
                            if "klines" in indicators:
                                self.binanceMerger.mergePriceOHLCV(spotTickers=spotTickers,
                                                                   umFuturesTickers=futuresTickers,
                                                                   timeframe=timeframe)
                        if "metrics" in indicators:
                            self.binanceMerger.mergeMetrics(UMFuturesTickers=futuresTickers)
                        if "fundingRate" in indicators:
                            self.binanceMerger.mergeFundingRates(UMFuturesTickers=futuresTickers)
                        return 1
                    except Exception as e:
                        self.logger.info(f"Failed to merge indicators to DB. Error: {e}")
                    finally:
                        try:
                            self._deleteDataFolder()
                        except Exception as e:
                            self.logger.info(f"Failed to delete CSV-files. Error: {e}")
                except Exception as e:
                    self.logger.info(f"Failed to dump indicators from exchange. Error: {e}")
                    return 0
            except Exception as e:
                self.logger.info(f"Failed to retrieve . Error: {e}")
                return 0
        except Exception as e:
            self.logger.info(f"Failed to fetch tickers from DB. Error: {e}")
            return 0

    def _deleteDataFolder(self, dataFolder = "data"):
        currentDir = os.getcwd()
        dataPath = os.path.join(currentDir, dataFolder)
        shutil.rmtree(dataPath)
        self.logger.info(f"CSV-files successfully deleted")


    def _updateDumperAttrs(self, spotTickers, futuresTickers, indicators, timeframes):
        self.binanceDumper.spotTickers = spotTickers
        self.binanceDumper.umFuturesTickers = futuresTickers
        self.binanceDumper.dataTypes = indicators
        self.binanceDumper.timeframes = timeframes
        self.binanceDumper._updateCurrentDate()

    def _dumpIndicators(self, fromDate=None):
        try:
            try:
                try:
                    self.binanceDumper.dumpData(dateStart=fromDate)
                    return 1
                except Exception as e:
                    self.logger.info(f"Failed to dump indicators. Error: {e}")
                    return 0
            except Exception as e:
                self.logger.info(f"Failed to retrieve tickers from DB. Error: {e}")
                return 0
        except Exception as e:
            self.logger.info(f"Failed to update NonIndicators. Error: {e}")
            return 0

    def _getSpotFuturesTickersFromDB(self):
        tickerDf = self.dbConnector.ticker.getTickersFromDB()
        spotTickers = tickerDf[tickerDf["isFutures"] == 0]["symbol"].to_list()
        futuresTickers = tickerDf[tickerDf["isFutures"] == 1]["symbol"].to_list()
        return spotTickers, futuresTickers

controller = Controller()
# controller.binanceDumper.dumpData()
controller.updateNonIndicators()
controller.updateIndicators(
    initialUpdate=True,
    # indicators=["fundingRate"]
        )


