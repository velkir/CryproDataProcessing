import pandas as pd
import os
import requests
import zipfile
import io
from queue import Queue
from threading import Thread

class BinanceDataDumper:
    _DATA_FREQUENCY_ENUM = ('1s','1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h',
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
    # _AGG_TRADES = "aggTrades/"

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
        else:
            raise Exception("Please provide correct timeframes")
        self.currentDate = pd.Timestamp.today(tz="utc")
        self.pathToSave = pathToSave if pathToSave else os.path.join(os.getcwd(), "data/")

    def dumpData(self, dateStart=None, dateEnd=None):
        queue = Queue()
        queue = self._getAllLinksPaths(queue=queue, dateStart=dateStart, dateEnd=dateEnd)
        numThreads = 10
        for i in range(numThreads):
            worker = Thread(target=self._download1File, args=(queue,))
            worker.start()
        queue.join()

    def _download1File(self, queue):
        while queue.not_empty:
            try:
                url, filename = queue.get_nowait()
                try:
                    response = requests.get(url)
                    if response.status_code == 200:
                        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                            file = z.namelist()[0]
                            os.makedirs(os.path.dirname(filename), exist_ok=True)
                            with z.open(file) as csv_file:
                                with open(filename, 'wb') as output_file:
                                    output_file.write(csv_file.read())
                            print(f"File {filename} created")
                    else:
                        print(f"There is no data for {url}")
                except Exception as e:
                    print(e)
                finally:
                    queue.task_done()
            except:
                print("Queue is empty, worker shuts down")
                break

    def _getAllLinksPaths(self, queue, dateStart=None, dateEnd=None):
        if self.spotTickers:
            if "klines" in self.dataTypes:
                for spotTicker in self.spotTickers:
                    queue = self._getKlinesLinksPaths(ticker=spotTicker, queue=queue,
                                                      spot=True, dataType="klines",
                                                      dateStart=dateStart, dateEnd=dateEnd)

        if self.umFuturesTickers:
            if "klines" in self.dataTypes:
                for umFuturesTicker in self.umFuturesTickers:
                    queue = self._getKlinesLinksPaths(ticker=umFuturesTicker, queue=queue,
                                                      dataType="klines", spot=False,
                                                      dateStart=dateStart, dateEnd=dateEnd)

            if "metrics" in self.dataTypes:
                for umFuturesTicker in self.umFuturesTickers:
                    queue = self._getKlinesLinksPaths(ticker=umFuturesTicker, queue=queue,
                                                      dataType="metrics", spot=False,
                                                      dateStart=dateStart, dateEnd=dateEnd)

            if "fundingRate" in self.dataTypes:
                for umFuturesTicker in self.umFuturesTickers:
                    queue = self._getKlinesLinksPaths(ticker=umFuturesTicker, queue=queue,
                                                      dataType="fundingRate", spot=False,
                                                      dateStart=dateStart, dateEnd=dateEnd)
        return queue

    def _getKlinesLinksPaths(self, ticker, queue, dataType="fundingRate",
                             spot=True, dateStart=None, dateEnd=None):
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
            fundingRateURLs = [f"{BinanceDataDumper.FUNDING_RATE_URL+ticker}/{ticker}-fundingRate-{date}.zip" for date in monthlyPrefixes]
            fundingRatePaths = [f"{self.pathToSave+BinanceDataDumper._FUTURES+BinanceDataDumper._UM+BinanceDataDumper._FUNDING_RATE}/{ticker}/{ticker}-fundingRate-{date}.csv" for date in monthlyPrefixes]
            queue = self._addURLsPathsToQueue(queue, fundingRateURLs, fundingRatePaths)
            return queue
        elif dataType == "metrics":
            dailyPrefixes = self._createDailyListOfDates(dateStart=dateStart, dateEnd=dateEnd)
            metricsURLs = [f"{BinanceDataDumper.METRICS_URL+ticker}/{ticker}-metrics-{date}.zip" for date in dailyPrefixes]
            metricsPaths = [f"{self.pathToSave+BinanceDataDumper._FUTURES+BinanceDataDumper._UM+BinanceDataDumper._METRICS}/{ticker}/{ticker}-metrics-{date}.csv" for date in dailyPrefixes]
            queue = self._addURLsPathsToQueue(queue, metricsURLs, metricsPaths)
            return queue

    def _createMonthlyListOfDates(self, dateStart=None, dateEnd=None):
        if dateStart is None:
            dateStart = self._setDefaultDate(dateStart=True)
        if dateEnd is None:
            dateEnd = self._setDefaultDate(dateStart=False)
        monthlyDates = pd.date_range(start=dateStart.strftime('%Y-%m'), end=dateEnd.strftime('%Y-%m'),
                                      freq="MS", inclusive="left").strftime('%Y-%m')
        if len(monthlyDates) > 0:
            return monthlyDates.to_list()
        else:
            return None

    def _createDailyListOfDates(self, dateStart=None, dateEnd=None):
        if dateEnd is None:
            dateEnd = self._setDefaultDate(dateStart=False)

        if dateStart is None and dateEnd.day == 1:
            return None
        elif dateStart is None and dateEnd.day != 1:
            daysDates = pd.date_range(start=pd.Timestamp(dateEnd.year, dateEnd.month, 1), end=dateEnd, freq="D").strftime('%Y-%m-%d')
            return daysDates.to_list()
        else:
            daysDates = pd.date_range(start=pd.Timestamp(dateStart), end=dateEnd,
                                      freq="D").strftime('%Y-%m-%d')
            return daysDates.to_list()

    def _setDefaultDate(self, dateStart=True):
        if dateStart:
            return pd.Timestamp(2017, 1, 9).date()
            # return pd.Timestamp(2023, 1, 1).date()
        else:
            return (self.currentDate-pd.Timedelta(days=1)).date()

    def _addURLsPathsToQueue(self, queue, urls, paths):
        for i in range(len(urls)):
            queue.put((urls[i], paths[i]))
        return queue


def loadTickersFromFile(path):
    with open(path, "r") as f:
        tickers = f.readlines()
        final_tickers = []
        # return [ticker.strip("'") for ticker in tickers]
        for ticker in tickers:
            final_tickers.append(ticker.strip("'\n"))
        return final_tickers

spotTickers = loadTickersFromFile("spotTickers.txt")

dumper = BinanceDataDumper(spotTickers=spotTickers,
                           dataTypes=["klines", "fundingRate", "metrics"],
                           timeframes=["1h"])
linksPaths = dumper.dumpData()