import pandas as pd
import os
import requests
import zipfile
import io
from queue import Queue
from threading import Thread

class BinanceDataDumper:
    _FUTURES_ASSET_CLASSES = ("um", "cm")
    _ASSET_CLASSES = ("spot")
    _DICT_DATA_TYPES_BY_ASSET = {
        "spot": ("klines"),
        "um_daily": ("klines", "metrics"),
        "um_monthly": ("klines", "fundingRate"),
        # "cm": ("klines")
    }
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
        self.timeframes = timeframes
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
                    queue = self._getKlinesLinksPaths(ticker=spotTicker, queue=queue, spot=True, dateStart=dateStart, dateEnd=dateEnd)

        if self.umFuturesTickers:
            if "klines" in self.dataTypes:
                for umFuturesTicker in self.umFuturesTickers:
                    queue = self._getKlinesLinksPaths(ticker=umFuturesTicker, queue=queue, spot=False, dateStart=dateStart, dateEnd=dateEnd)

            if "metrics" in self.dataTypes:
                for umFuturesTicker in self.umFuturesTickers:
                    pass

            if "fundingRate" in self.dataTypes:
                for umFuturesTicker in self.umFuturesTickers:
                    pass
        return queue

    def _getKlinesLinksPaths(self, ticker, queue, spot=True, dateStart=None, dateEnd=None):
        if spot:
            klines_monthly = BinanceDataDumper.KLINES_SPOT_MONTHLY_URL
            klines_daily = BinanceDataDumper.KLINES_SPOT_DAILY_URL
            market = BinanceDataDumper._SPOT
        else:
            klines_monthly = BinanceDataDumper.KLINES_UM_FUTURES_MONTHLY_URL
            klines_daily = BinanceDataDumper.KLINES_UM_FUTURES_DAILY_URL
            market = BinanceDataDumper._FUTURES+BinanceDataDumper._UM

        monthlyPrefixes = self._createMonthlyListOfDates(dateStart=dateStart, dateEnd=dateEnd)
        dailyPrefixes = self._createDailyListOfDates(dateEnd=dateEnd)
        for timeframe in self.timeframes:
            monthlyLinks = [f"{klines_monthly + ticker}/{timeframe}/{ticker}-{timeframe}-{date}.zip" for date in monthlyPrefixes]
            dailyLinks = [f"{klines_daily + ticker}/{timeframe}/{ticker}-{timeframe}-{date}.zip" for date in dailyPrefixes]

            monthlyPaths = [f"{self.pathToSave + market + BinanceDataDumper._KLINES + ticker}/{timeframe}/{ticker}-{timeframe}-{date}.csv" for date in monthlyPrefixes]
            dailyPaths = [f"{self.pathToSave + market + BinanceDataDumper._KLINES + ticker}/{timeframe}/{ticker}-{timeframe}-{date}.csv" for date in dailyPrefixes]

            for i in range(len(monthlyLinks)):
                queue.put((monthlyLinks[i], monthlyPaths[i]))

            for i in range(len(dailyLinks)):
                queue.put((dailyLinks[i], dailyPaths[i]))
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

    def _createDailyListOfDates(self, dateEnd=None):
        if dateEnd is None:
            dateEnd = self._setDefaultDate(dateStart=False)
        if dateEnd.day == 1:
            return None
        else:
            daysDates = pd.date_range(start=pd.Timestamp(dateEnd.year, dateEnd.month, 1), end=dateEnd, freq="D").strftime('%Y-%m-%d')
            return daysDates.to_list()

    def _setDefaultDate(self, dateStart=True):
        if dateStart:
            return pd.Timestamp(2017, 1, 9).date()
            # return pd.Timestamp(2023, 1, 1).date()
        else:
            return (self.currentDate-pd.Timedelta(days=1)).date()

# dumper = BinanceDataDumper(spotTickers=["BTCUSDT", "ETHUSDT"], umFuturesTickers=["BTCUSDT", "ETHUSDT"])
dumper = BinanceDataDumper(spotTickers=["BTCUSDT"])
linksPaths = dumper.dumpData()