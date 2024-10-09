import pandas as pd
import os
import requests
import zipfile
import io
from queue import Queue
from threading import Thread
import logging
import threading
import warnings
import sys


class BinanceDataDumper:
    _DATA_FREQUENCY_ENUM = ('1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h',
                            '1d', '3d', '1w', '1mo')
    _BASE_URL = "https://data.binance.vision/data/"
    _SPOT = "spot/"
    _FUTURES = "futures/"
    _UM = "um/"
    _COIN = "cm/"
    _DAILY = "daily/"
    _MONTHLY = "monthly/"
    _KLINES = "klines/"
    _METRICS = "metrics/"
    _FUNDING_RATE = "fundingRate/"

    KLINES_SPOT_DAILY_URL = _BASE_URL + _SPOT + _DAILY + _KLINES
    KLINES_SPOT_MONTHLY_URL = _BASE_URL + _SPOT + _MONTHLY + _KLINES
    KLINES_UM_FUTURES_DAILY_URL = _BASE_URL + _FUTURES + _UM + _DAILY + _KLINES
    KLINES_UM_FUTURES_MONTHLY_URL = _BASE_URL + _FUTURES + _UM + _MONTHLY + _KLINES
    METRICS_URL = _BASE_URL + _FUTURES + _UM + _DAILY + _METRICS
    FUNDING_RATE_URL = _BASE_URL + _FUTURES + _UM + _MONTHLY + _FUNDING_RATE

    def __init__(self, spotTickers=None, umFuturesTickers=None, dataTypes=["klines", "fundingRate"], timeframes=["1m"], pathToSave=None):
        self.spotTickers = spotTickers
        self.umFuturesTickers = umFuturesTickers
        self.dataTypes = dataTypes
        if all([timeframe in BinanceDataDumper._DATA_FREQUENCY_ENUM for timeframe in timeframes]):
            self.timeframes = timeframes
            logger.info(f"Timeframes set to: {self.timeframes}")
        else:
            logger.error("Incorrect timeframes provided")
            raise Exception("Please provide correct timeframes")
        self.currentDate = pd.Timestamp.today(tz="utc")
        self.pathToSave = pathToSave if pathToSave else os.path.join(os.getcwd(), "data/")
        logger.info(f"Data will be saved to: {self.pathToSave}")

    def dumpData(self, dateStart=None, dateEnd=None):
        logger.info("Starting data download process")
        queue = Queue()
        queue = self._getAllLinksPaths(queue=queue, dateStart=dateStart, dateEnd=dateEnd)
        numThreads = 25
        logger.info(f"Launching {numThreads} threads for data download")
        for i in range(numThreads):
            worker = Thread(target=self._download1File, args=(queue,))
            worker.start()
        queue.join()
        logger.info("Data download process completed")

    def _download1File(self, queue):
        while queue.not_empty:
            try:
                url, filename = queue.get_nowait()
                try:
                    response = requests.get(url)
                    if response.status_code == 200:
                        logger.info(f"Downloaded file from URL: {url}")
                        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                            file = z.namelist()[0]
                            os.makedirs(os.path.dirname(filename), exist_ok=True)
                            with z.open(file) as csv_file:
                                with open(filename, 'wb') as output_file:
                                    output_file.write(csv_file.read())
                        logger.info(f"File {filename} created")
                    else:
                        logger.info(f"No data available for {url}")
                except Exception as e:
                    logger.error(f"Error downloading {url}: {e}")
                finally:
                    queue.task_done()
            except:
                logger.info("Queue is empty, thread shutting down")
                break

    def _getAllLinksPaths(self, queue, dateStart=None, dateEnd=None):
        logger.info("Gathering all links and paths for download")
        if self.spotTickers:
            if "klines" in self.dataTypes:
                for spotTicker in self.spotTickers:
                    logger.info(f"Processing spot ticker: {spotTicker}")
                    queue = self._getKlinesLinksPaths(ticker=spotTicker, queue=queue,
                                                      spot=True, dataType="klines",
                                                      dateStart=dateStart, dateEnd=dateEnd)

        if self.umFuturesTickers:
            if "klines" in self.dataTypes:
                for umFuturesTicker in self.umFuturesTickers:
                    logger.info(f"Processing futures ticker: {umFuturesTicker} for klines")
                    queue = self._getKlinesLinksPaths(ticker=umFuturesTicker, queue=queue,
                                                      dataType="klines", spot=False,
                                                      dateStart=dateStart, dateEnd=dateEnd)

            if "metrics" in self.dataTypes:
                for umFuturesTicker in self.umFuturesTickers:
                    logger.info(f"Processing futures ticker: {umFuturesTicker} for metrics")
                    queue = self._getKlinesLinksPaths(ticker=umFuturesTicker, queue=queue,
                                                      dataType="metrics", spot=False,
                                                      dateStart=dateStart, dateEnd=dateEnd)

            if "fundingRate" in self.dataTypes:
                for umFuturesTicker in self.umFuturesTickers:
                    logger.info(f"Processing futures ticker: {umFuturesTicker} for fundingRate")
                    queue = self._getKlinesLinksPaths(ticker=umFuturesTicker, queue=queue,
                                                      dataType="fundingRate", spot=False,
                                                      dateStart=dateStart, dateEnd=dateEnd)
        return queue

    def _getKlinesLinksPaths(self, ticker, queue, dataType="fundingRate",
                             spot=True, dateStart=None, dateEnd=None):
        logger.info(f"Getting links and paths for ticker {ticker}, data type {dataType}")
        if spot:
            if dataType == "klines":
                klinesMonthly = BinanceDataDumper.KLINES_SPOT_MONTHLY_URL
                klinesDaily = BinanceDataDumper.KLINES_SPOT_DAILY_URL
                market = BinanceDataDumper._SPOT
        else:
            if dataType == "klines":
                klinesMonthly = BinanceDataDumper.KLINES_UM_FUTURES_MONTHLY_URL
                klinesDaily = BinanceDataDumper.KLINES_UM_FUTURES_DAILY_URL
                market = BinanceDataDumper._FUTURES + BinanceDataDumper._UM

        if dataType == "klines":
            monthlyPrefixes = self._createMonthlyListOfDates(dateStart=dateStart, dateEnd=dateEnd)
            dailyPrefixes = self._createDailyListOfDates(dateEnd=dateEnd)
            for timeframe in self.timeframes:
                monthlyLinks = [f"{klinesMonthly + ticker}/{timeframe}/{ticker}-{timeframe}-{date}.zip" for date in monthlyPrefixes]
                dailyLinks = [f"{klinesDaily + ticker}/{timeframe}/{ticker}-{timeframe}-{date}.zip" for date in dailyPrefixes]

                monthlyPaths = [f"{self.pathToSave + market + BinanceDataDumper._KLINES + ticker}/{timeframe}/{ticker}-{timeframe}-{date}.csv" for date in monthlyPrefixes]
                dailyPaths = [f"{self.pathToSave + market + BinanceDataDumper._KLINES + ticker}/{timeframe}/{ticker}-{timeframe}-{date}.csv" for date in dailyPrefixes]

                queue = self._addURLsPathsToQueue(queue, monthlyLinks, monthlyPaths)
                queue = self._addURLsPathsToQueue(queue, dailyLinks, dailyPaths)
            return queue
        elif dataType == "fundingRate":
            monthlyPrefixes = self._createMonthlyListOfDates(dateStart=dateStart, dateEnd=dateEnd)
            fundingRateURLs = [f"{BinanceDataDumper.FUNDING_RATE_URL + ticker}/{ticker}-fundingRate-{date}.zip" for date in monthlyPrefixes]
            fundingRatePaths = [f"{self.pathToSave + BinanceDataDumper._FUTURES + BinanceDataDumper._UM + BinanceDataDumper._FUNDING_RATE}/{ticker}/{ticker}-fundingRate-{date}.csv" for date in monthlyPrefixes]
            queue = self._addURLsPathsToQueue(queue, fundingRateURLs, fundingRatePaths)
            return queue
        elif dataType == "metrics":
            dailyPrefixes = self._createDailyListOfDates(dateStart=dateStart, dateEnd=dateEnd, metrics=True)
            metricsURLs = [f"{BinanceDataDumper.METRICS_URL + ticker}/{ticker}-metrics-{date}.zip" for date in dailyPrefixes]
            metricsPaths = [f"{self.pathToSave + BinanceDataDumper._FUTURES + BinanceDataDumper._UM + BinanceDataDumper._METRICS}/{ticker}/{ticker}-metrics-{date}.csv" for date in dailyPrefixes]
            queue = self._addURLsPathsToQueue(queue, metricsURLs, metricsPaths)
            return queue

    def _createMonthlyListOfDates(self, dateStart=None, dateEnd=None):
        if dateStart is None:
            dateStart = self._setDefaultDate(dateStart=True)
        if dateEnd is None:
            dateEnd = self._setDefaultDate(dateStart=False)
        logger.info(f"Creating monthly date list from {dateStart} to {dateEnd}")
        monthlyDates = pd.date_range(start=dateStart.strftime('%Y-%m'), end=dateEnd.strftime('%Y-%m'),
                                     freq="MS", inclusive="left").strftime('%Y-%m')
        if len(monthlyDates) > 0:
            return monthlyDates.to_list()
        else:
            return []

    def _createDailyListOfDates(self, dateStart=None, dateEnd=None, metrics=False):
        if dateEnd is None:
            dateEnd = self._setDefaultDate(dateStart=False)

        if not metrics:
            if dateStart is None and dateEnd.day == 1:
                return []
            elif dateStart is None and dateEnd.day != 1:
                daysDates = pd.date_range(start=pd.Timestamp(dateEnd.year, dateEnd.month, 1), end=dateEnd, freq="D").strftime('%Y-%m-%d')
                logger.info(f"Creating daily date list from start of month to {dateEnd}")
                return daysDates.to_list()
            else:
                daysDates = pd.date_range(start=pd.Timestamp(dateStart), end=dateEnd,
                                          freq="D").strftime('%Y-%m-%d')
                logger.info(f"Creating daily date list from {dateStart} to {dateEnd}")
                return daysDates.to_list()
        else:
            if dateStart is None:
                dateStart = pd.Timestamp(2017, 1, 9).date()
            daysDates = pd.date_range(start=dateStart, end=dateEnd, freq="D").strftime('%Y-%m-%d')
            logger.info(f"Creating daily date list for metrics from {dateStart} to {dateEnd}")
            return daysDates.to_list()

    def _setDefaultDate(self, dateStart=True):
        if dateStart:
            defaultDate = pd.Timestamp(2017, 1, 9).date()
        else:
            defaultDate = (self.currentDate - pd.Timedelta(days=1)).date()
        logger.info(f"Default date set to: {defaultDate}")
        return defaultDate

    def _addURLsPathsToQueue(self, queue, urls, paths):
        logger.info(f"Adding {len(urls)} URLs and paths to the queue")
        for i in range(len(urls)):
            queue.put((urls[i], paths[i]))
        return queue


def loadTickersFromFile(path):
    logger.info(f"Loading tickers from file: {path}")
    with open(path, "r") as f:
        tickers = f.readlines()
        final_tickers = []
        for ticker in tickers:
            final_tickers.append(ticker.strip("'\n"))
    logger.info(f"Loaded {len(final_tickers)} tickers")
    return final_tickers

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

file_handler = logging.FileHandler('BinanceDataDumper.log', mode='a', encoding='utf-8')
file_handler.setLevel(logging.INFO)

# Create formatter with timestamp
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Set formatter for handlers
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)


# spotTickers = loadTickersFromFile("spotTickers.txt")
# umFuturesTickers = loadTickersFromFile("umFuturesTickers.txt")

dumper = BinanceDataDumper(
    # spotTickers=["BTCUSDT", "ETHUSDT"],
    umFuturesTickers=["DOGEUSDT", "1000PEPEUSDT", "1000SHIBUSDT", "TRXUSDT", "EOSUSDT", "PROMUSDT"],
    # dataTypes=["klines", "fundingRate", "metrics"],
    dataTypes=["metrics", "fundingRate"],
    timeframes=["4h"]
    # timeframes=["1m"]
)
dumper.dumpData()


# spotTickers = loadTickersFromFile("spotTickers.txt")
# umFuturesTickers = loadTickersFromFile("umFuturesTickers.txt")
# dumper = BinanceDataDumper(
#                            # spotTickers=["BTCUSDT", "ETHUSDT"],
#                            umFuturesTickers=["DOGEUSDT", "1000PEPEUSDT", "1000SHIBUSDT", "TRXUSDT", "EOSUSDT", "PROMUSDT"],
#                            # umFuturesTickers=["BTCUSDT", "ETHUSDT"],
#                            # dataTypes=["klines", "fundingRate", "metrics"],
#                            # dataTypes=["klines"],
#                            dataTypes=["klines"]
#                            # timeframes=["1m"]
#                             )
# linksPaths = dumper.dumpData()