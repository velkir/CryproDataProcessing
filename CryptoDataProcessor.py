import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import ccxt
from playwright.sync_api import sync_playwright
from sqlalchemy import event
import threading
import time
import random
from sqlalchemy.exc import InternalError
from mysql.connector.errors import InternalError as MySQLInternalError
import tempfile

INDICATOR_TABLES = ["PriceOHLCV", "OpenInterest", "FundingRate", "LongShortRatio", "LiquidationAggregated"]

class CryptoDataProcessor:
    def __init__(self, dbName, logger):
        self.logger = logger
        self.logger.info("Initializing CryptoDataProcessor with database: %s", dbName)
        self._engine = self._initialize_db_connection(dbName)
        self._register_event()
        self.ccxtBinance = ccxt.binance()
        self.exchange = Exchange(self)
        self.exchangeBalanceHistory = ExchangeBalanceHistory(self)
        self.asset = Asset(self)
        self.assetTag = AssetTag(self)
        self.assetGroup = AssetGroup(self)
        self.assetHistory = AssetHistory(self)
        self.ticker = Ticker(self)
        self.tickerGroup = TickerGroup(self)
        self.indicators = Indicators(self)
        self.logger.info("CryptoDataProcessor initialized successfully.")

    def _initialize_db_connection(self, dbName):
        self.logger.info("Initializing database connection to %s", dbName)
        connection_string = f"mysql+mysqlconnector://user:1234@localhost:3306/{dbName}"
        engine = create_engine(
            connection_string,
            echo=False,
            # echo=True,
            connect_args={
                'allow_local_infile': True  # Включаем загрузку локальных файлов на стороне клиента
            }
        )
        self.logger.info("Database connection established.")
        return engine

    def _basic_query(self, query, queryType="modify", params=None):
        thread_name = threading.current_thread().name
        self.logger.info(f"Thread {thread_name}: Executing basic query: {query}")
        try:
            with self._engine.connect() as con:
                result = con.execute(text(query), params)
                con.commit()
                if queryType == "modify":
                    self.logger.info(f"Thread {thread_name}: Query executed successfully, affected rows: {result.rowcount}")
                    return result.rowcount
                elif queryType == "select":
                    fetch_result = result.fetchall()
                    self.logger.info(f"Thread {thread_name}: Query executed successfully, fetched rows: {len(fetch_result)}")
                    return fetch_result
        except Exception as e:
            self.logger.exception(f"Thread {thread_name}: An error occurred during basic query execution: {e}")

    def _select_pandas_query(self, query, params=None, parse_dates=None, columns=None, chunksize=50000):
        thread_name = threading.current_thread().name
        self.logger.info(f"Thread {thread_name}: Executing pandas select query: {query}")
        try:
            with self._engine.connect() as con:
                if chunksize:
                    # Если задан chunksize, читаем данные порциями и объединяем их внутри функции
                    df_iterator = pd.read_sql(
                        sql=query,
                        con=con,
                        params=params,
                        parse_dates=parse_dates,
                        columns=columns,
                        chunksize=chunksize
                    )
                    # Объединяем чанки в один DataFrame
                    chunks = []
                    for df_chunk in df_iterator:
                        # Можно выполнить предварительную обработку каждого чанка здесь
                        chunks.append(df_chunk)
                    df = pd.concat(chunks, ignore_index=True)
                    self.logger.info(f"Thread {thread_name}: Pandas query executed successfully, fetched rows: {len(df)}")
                    return df
                else:
                    # Если chunksize не задан, читаем данные сразу
                    df = pd.read_sql(
                        sql=query,
                        con=con,
                        params=params,
                        parse_dates=parse_dates,
                        columns=columns
                    )
                    self.logger.info(f"Thread {thread_name}: Pandas query executed successfully, fetched rows: {len(df)}")
                    return df
        except Exception as e:
            self.logger.exception(f"Thread {thread_name}: An error occurred during pandas select query execution: {e}")
            return None

    def _modify_query_pandas(self, df, table, if_exists='append', index=False):
        thread_name = threading.current_thread().name
        self.logger.info(
            f"Thread {thread_name}: Executing optimized modify query on table {table} with if_exists='{if_exists}'")

        # Максимальное количество повторных попыток
        max_retries = 5
        retry_count = 0

        # Создаем временный CSV-файл в безопасном каталоге временных файлов
        temp_dir = tempfile.gettempdir()
        temp_csv_file = os.path.join(temp_dir, f"temp_{thread_name}_{table}.csv")

        # Убеждаемся, что индекс не записывается в CSV-файл
        df.to_csv(temp_csv_file, index=False, header=False)

        # Получаем список столбцов DataFrame
        df_columns = df.columns.tolist()

        # Получаем информацию о структуре таблицы из базы данных
        with self._engine.connect() as conn:
            result = conn.execute(text(f"SHOW COLUMNS FROM `{table}`"))
            table_columns_info = result.fetchall()

        # Создаем список столбцов таблицы и определяем автоинкрементные столбцы
        table_columns = []
        auto_increment_columns = []
        for column_info in table_columns_info:
            column_name = column_info[0]
            extra = column_info[5]  # Поле 'Extra' в результате SHOW COLUMNS
            table_columns.append(column_name)
            if 'auto_increment' in extra:
                auto_increment_columns.append(column_name)

        # Исключаем автоинкрементные столбцы из списка столбцов для загрузки
        load_columns = [col for col in df_columns if col not in auto_increment_columns]
        columns_str = ', '.join([f"`{col}`" for col in load_columns])  # Оборачиваем имена столбцов в обратные кавычки

        # Создаем SET выражения для автоинкрементных столбцов
        set_expressions = ', '.join([f"`{col}` = NULL" for col in auto_increment_columns])  # Оборачиваем имена столбцов

        while retry_count <= max_retries:
            try:
                with self._engine.connect() as conn:
                    # Отключаем проверки внешних ключей и автокоммит
                    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
                    conn.execute(text("SET AUTOCOMMIT = 0"))

                    if if_exists == 'replace':
                        self.logger.info(f"Thread {thread_name}: Replacing data in table {table}")
                        # Очищаем таблицу перед загрузкой данных
                        conn.execute(text(f"TRUNCATE TABLE `{table}`"))
                    elif if_exists == 'fail':
                        # Проверяем, есть ли данные в таблице
                        result = conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1")).fetchone()
                        if result:
                            self.logger.error(
                                f"Thread {thread_name}: Table {table} already contains data. Aborting due to if_exists='fail'.")
                            # Удаляем временный CSV-файл
                            os.remove(temp_csv_file)
                            raise Exception(f"Table {table} already contains data. Aborting due to if_exists='fail'.")

                    # Преобразуем путь к файлу для MySQL
                    mysql_compatible_path = temp_csv_file.replace('\\', '/')

                    # Загружаем данные с помощью LOAD DATA LOCAL INFILE, указывая столбцы
                    load_sql = f"""
                        LOAD DATA LOCAL INFILE '{mysql_compatible_path}'
                        INTO TABLE `{table}`
                        FIELDS TERMINATED BY ',' 
                        LINES TERMINATED BY '\\n'
                        ({columns_str})
                    """

                    # Добавляем SET выражения для автоинкрементных столбцов
                    if set_expressions:
                        load_sql += f" SET {set_expressions};"
                    else:
                        load_sql += ";"

                    conn.execute(text(load_sql).execution_options(autocommit=True))

                    # Включаем проверки внешних ключей и автокоммит
                    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
                    conn.execute(text("COMMIT"))
                    conn.execute(text("SET AUTOCOMMIT = 1"))

                # Удаляем временный CSV-файл
                os.remove(temp_csv_file)
                self.logger.info(f"Thread {thread_name}: Data loaded successfully using LOAD DATA INFILE.")
                break  # Если успешно, выходим из цикла
            except (InternalError, MySQLInternalError) as e:
                if '1213' in str(e):
                    retry_count += 1
                    if retry_count > max_retries:
                        self.logger.error(f"Thread {thread_name}: Max retries exceeded. Could not resolve deadlock.")
                        # Удаляем временный CSV-файл
                        os.remove(temp_csv_file)
                        raise
                    wait_time = random.uniform(0.5, 2.0)
                    self.logger.warning(
                        f"Thread {thread_name}: Deadlock detected. Retrying {retry_count}/{max_retries} after {wait_time:.2f} seconds.")
                    time.sleep(wait_time)
                else:
                    self.logger.exception(
                        f"Thread {thread_name}: An error occurred during optimized modify query execution: {e}")
                    # Удаляем временный CSV-файл
                    os.remove(temp_csv_file)
                    raise
            except Exception as e:
                self.logger.exception(f"Thread {thread_name}: An unexpected error occurred: {e}")
                # Удаляем временный CSV-файл
                os.remove(temp_csv_file)
                raise

    def _getSymbolId(self, symbol, exchangeName="Binance", spot=True):
        self.logger.info("Fetching symbol ID for symbol: %s", symbol)
        if spot:
            isFutures = 0
        else:
            isFutures = 1
        query = f"SELECT symbolId FROM Ticker WHERE symbol='{symbol}' AND exchangeName='{exchangeName}' AND isFutures={isFutures};"
        result = self._basic_query(query, queryType="select")
        if result:
            self.logger.info("Symbol ID found: %s for symbol: %s", result[0][0], symbol)
            return result[0][0]
        else:
            self.logger.info("Symbol ID not found for symbol: %s", symbol)
            return None

    def _register_event(self):
        self.logger.info("Registering database event for fast executemany.")
        @event.listens_for(self._engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True
        self.logger.info("Database event registered.")

class Exchange:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.dataProcessor.logger.info("Initializing Exchange class.")

    def add_exchange(self, exchangeName):
        self.dataProcessor.logger.info("Adding exchange: %s", exchangeName)
        query = f"INSERT INTO Exchange (exchangeName) VALUES ('{exchangeName}');"
        return self.dataProcessor._basic_query(query)

    def delete_exchange(self, exchangeName):
        self.dataProcessor.logger.info("Deleting exchange: %s", exchangeName)
        query = f"DELETE FROM Exchange WHERE exchangeName='{exchangeName}';"
        return self.dataProcessor._basic_query(query)

    def getExchange(self, exchangeName):
        self.dataProcessor.logger.info("Fetching exchange: %s", exchangeName)
        query = f"SELECT * FROM Exchange WHERE exchangeName = '{exchangeName}';"
        result = self.dataProcessor._select_pandas_query(query=query)
        if len(result) > 0:
            self.dataProcessor.logger.info("Exchange found: %s", exchangeName)
            return result
        else:
            self.dataProcessor.logger.info("Exchange not found: %s", exchangeName)
            return None

    def getAllExchanges(self, params=None, parse_dates=None, columns=None, chunksize=None):
        self.dataProcessor.logger.info("Fetching all exchanges.")
        query = f"SELECT * FROM Exchange;"
        return self.dataProcessor._select_pandas_query(query=query,
                                                       params=params,
                                                       parse_dates=parse_dates,
                                                       columns=columns,
                                                       chunksize=chunksize)

class ExchangeBalanceHistory:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.dataProcessor.logger.info("Initializing ExchangeBalanceHistory class.")

    def updateExchangeBalance(self, exchangeName):
        self.dataProcessor.logger.info("Updating exchange balance for: %s", exchangeName)
        # Логика запроса к coinglass  https://docs.coinglass.com/reference/exchange-balance-list
        # Логика обработки данных в pd.df и добавление в БД
        pass

    def getExchangeBalanceHistoryRecords(self, exchangeName, fromDatetime=None):
        self.dataProcessor.logger.info("Fetching exchange balance history records for: %s", exchangeName)
        base_query = f"SELECT * FROM ExchangeBalanceHistory WHERE exchangeName='{exchangeName}'"
        params = ""
        closing_part = " ORDER BY timestamp;"
        if fromDatetime is not None:
            params = f" AND timestamp >= '{fromDatetime}'"
        query = base_query + params + closing_part
        return self.dataProcessor._select_pandas_query(query=query)

    def getLastExchangeBalanceHistoryRecord(self, exchangeName):
        self.dataProcessor.logger.info("Fetching last exchange balance history record for: %s", exchangeName)
        query = f"SELECT balance FROM ExchangeBalanceHistory WHERE exchangeName='{exchangeName}' ORDER BY timestamp DESC LIMIT 1;"
        result = self.dataProcessor._basic_query(query=query, queryType="select")
        if result:
            self.dataProcessor.logger.info("Last balance record found for exchange: %s", exchangeName)
            return result[0][0]
        else:
            self.dataProcessor.logger.info("No balance record found for exchange: %s", exchangeName)
            return None

class Asset:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.dataProcessor.logger.info("Initializing Asset class.")
        self.binance_assets_url = "https://www.binance.com/bapi/apex/v1/friendly/apex/marketing/complianceSymbolList"

    def getAssetsFromDB(self):
        self.dataProcessor.logger.info("Fetching assets from database.")
        query = f"SELECT * FROM Asset ORDER BY cmcId"
        return self.dataProcessor._select_pandas_query(query=query)

    def _getAssetsFromExchange(self, exchangeName="Binance", assetHistory=False):
        self.dataProcessor.logger.info("Fetching assets from exchange: %s", exchangeName)
        try:
            if exchangeName == "Binance":
                request_data = self._makeRequestAssets()
                assetsDfAllColumns = pd.DataFrame(request_data["data"])
                assetsDfAllColumns.rename(columns={"name": "assetName", "cmcUniqueId": "cmcId", "tags": "tag"}, inplace=True)
                if assetHistory:
                    assetsDfAllColumns["timestamp"] = pd.Timestamp.now(tz="utc")
                    return assetsDfAllColumns[
                        ["assetName", "timestamp", "cmcId", "marketcap", "circulatingSupply", "maxSupply", "totalSupply"]]
                else:
                    return assetsDfAllColumns[["assetName", "cmcId", "tag"]]
            else:
                self.dataProcessor.logger.error("Exchange not implemented: %s", exchangeName)
                raise NotImplementedError("Добавлен только Binance")
        except Exception as e:
            self.dataProcessor.logger.exception(f"An error occurred while fetching assets from exchange: {e}")

    def _makeRequestAssets(self):
        self.dataProcessor.logger.info("Making request to Binance assets URL.")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            page.goto('https://www.binance.com')
            page.wait_for_load_state('networkidle')
            response = page.request.get(self.binance_assets_url)
            data = response.json()
            browser.close()
            self.dataProcessor.logger.info("Received data from Binance assets URL.")
            return data

    def updateAssets(self, exchangeName="Binance", DBAssets=None, exchangeAssets=None):
        self.dataProcessor.logger.info("Updating assets from exchange: %s", exchangeName)
        if exchangeAssets is None:
            exchangeAssets = self._getAssetsFromExchange(exchangeName)
        if DBAssets is None:
            DBAssets = self.getAssetsFromDB()
        newAssets = exchangeAssets[~exchangeAssets["cmcId"].isin(DBAssets["cmcId"])]
        newAssets_without_tags = newAssets.drop(columns="tag")
        if not newAssets.empty:
            self.dataProcessor.logger.info("New assets found, adding to database.")
            assets_affected_rows = self.dataProcessor._modify_query_pandas(newAssets_without_tags, "Asset",
                                                                           if_exists='append', index=False)
            df_tags = newAssets[["assetName", "tag"]].explode("tag")
            df_tags.dropna(inplace=True)
            tags_affected_rows = self.dataProcessor.assetTag.addTags(df_tags)
            self.dataProcessor.logger.info("Assets updated successfully.")
            return assets_affected_rows, tags_affected_rows
        else:
            self.dataProcessor.logger.info("No new assets to update.")
            return None

class AssetTag:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.dataProcessor.logger.info("Initializing AssetTag class.")

    def addTags(self, df_tags):
        self.dataProcessor.logger.info("Adding asset tags to database.")
        return self.dataProcessor._modify_query_pandas(df_tags, "AssetTag", if_exists='append', index=False)

    def getTags(self, assetsList=None):
        self.dataProcessor.logger.info("Fetching asset tags.")
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
        self.dataProcessor.logger.info("Initializing AssetGroup class.")

    def addAssetGroup(self, assetGroupName):
        self.dataProcessor.logger.info("Adding asset group: %s", assetGroupName)
        query = f"INSERT INTO AssetGroup (assetGroupName) VALUES ('{assetGroupName}');"
        return self.dataProcessor._basic_query(query=query)

    def deleteAssetGroup(self, assetGroupName):
        self.dataProcessor.logger.info("Deleting asset group: %s", assetGroupName)
        query = f"DELETE FROM AssetGroup WHERE assetGroupName = '{assetGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def renameAssetGroup(self, oldAssetGroupName, newAssetGroupName):
        self.dataProcessor.logger.info("Renaming asset group from %s to %s", oldAssetGroupName, newAssetGroupName)
        query = f"UPDATE AssetGroup SET assetGroupName = '{newAssetGroupName}' WHERE assetGroupName = '{oldAssetGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def getAllAssetGroups(self):
        self.dataProcessor.logger.info("Fetching all asset groups.")
        query = f"SELECT * FROM AssetGroup;"
        return self.dataProcessor._select_pandas_query(query=query)

    def addAssetsToAssetGroup(self, assetGroupName, assetsList):
        self.dataProcessor.logger.info("Adding assets to asset group: %s", assetGroupName)
        assetGroupId = self._getAssetGroupId(assetGroupName)
        addedAssetsCount = 0
        for asset in assetsList:
            query = f"INSERT INTO AssetGroupLinkTable (assetGroupId, assetName) VALUES ({assetGroupId}, '{asset}');"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                addedAssetsCount += success
        self.dataProcessor.logger.info("Added %d assets to asset group: %s", addedAssetsCount, assetGroupName)
        return addedAssetsCount

    def deleteAssetsFromAssetGroup(self, assetGroupName, assetsList):
        self.dataProcessor.logger.info("Deleting assets from asset group: %s", assetGroupName)
        assetGroupId = self._getAssetGroupId(assetGroupName)
        deletedAssetsCount = 0
        for asset in assetsList:
            query = f"DELETE FROM AssetGroupLinkTable WHERE (assetGroupId={assetGroupId} AND assetName='{asset}');"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                deletedAssetsCount += success
        self.dataProcessor.logger.info("Deleted %d assets from asset group: %s", deletedAssetsCount, assetGroupName)
        return deletedAssetsCount

    def getAssetsFromAssetGroup(self, assetGroupName):
        self.dataProcessor.logger.info("Fetching assets from asset group: %s", assetGroupName)
        assetGroupId = self._getAssetGroupId(assetGroupName)
        query = f"SELECT assetName FROM assetGroupLinkTable WHERE assetGroupId = {assetGroupId};"
        return self.dataProcessor._select_pandas_query(query=query)

    def _getAssetGroupId(self, assetGroupName):
        self.dataProcessor.logger.info("Fetching asset group ID for: %s", assetGroupName)
        query = f"SELECT assetGroupId FROM AssetGroup WHERE assetGroupName = '{assetGroupName}';"
        return self.dataProcessor._basic_query(query=query, queryType="select")[0][0]

class AssetHistory:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.dataProcessor.logger.info("Initializing AssetHistory class.")

    def addAssetsHistoryRecords(self):
        self.dataProcessor.logger.info("Adding asset history records.")
        df = self.dataProcessor.asset._getAssetsFromExchange(assetHistory=True)
        return self.dataProcessor._modify_query_pandas(df, "AssetHistoryRecords")

    def getAssetHistoryRecords(self, assetName, fromDatetime=None):
        self.dataProcessor.logger.info("Fetching asset history records for: %s", assetName)
        base_query = f"SELECT * FROM AssetHistory WHERE assetName = '{assetName}'"
        if fromDatetime:
            params = f" AND Timestamp >= '{fromDatetime}';"
        else:
            params = ";"
        query = base_query + params
        result = self.dataProcessor._select_pandas_query(query=query)
        if len(result) > 0:
            self.dataProcessor.logger.info("Asset history records found for: %s", assetName)
            return result
        else:
            self.dataProcessor.logger.info("No asset history records found for: %s", assetName)
            return None

class Ticker:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.dataProcessor.logger.info("Initializing Ticker class.")

    def getTickersFromDB(self, symbolsList=None, params=None):
        self.dataProcessor.logger.info("Fetching tickers from database.")
        query = f"SELECT * FROM Ticker"
        params = ";"
        if symbolsList:
            params = f" WHERE {''.join(['symbol = ' + '\'' + str(symbol) + '\' OR ' for symbol in symbolsList])}"
            params = params[:-4] + ";"
        query += params
        return self.dataProcessor._select_pandas_query(query)

    def updateTickers(self):
        self.dataProcessor.logger.info("Updating tickers.")
        df_exchange_tickers = self.getTickersFromExchange()
        df_db_tickers = self.getTickersFromDB()
        df_db_tickers.drop(columns=["symbolId"], inplace=True)
        merged_df = pd.merge(df_exchange_tickers,
                             df_db_tickers,
                             on=['symbol', 'exchangeName', 'isFutures'],
                             # how='outer',
                             how='left',
                             indicator=True,
                             suffixes=('', '_y'))
        newTickers = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

        # newTickers = merged_df[merged_df['_merge'] != 'both'].drop(columns=['_merge'])

        columns_to_drop = [column for column in newTickers.columns if column.endswith("_y")]
        newTickers.drop(columns=columns_to_drop,
                        inplace=True)
        if not newTickers.empty:
            self.dataProcessor.logger.info("New tickers found, adding to database.")
            return self.addTickers(newTickers)
        else:
            self.dataProcessor.logger.info("No new tickers to update.")
            return None

    def getTickersFromExchange(self, exchangeName="Binance", whiteListedQuoteAssets=True):
        self.dataProcessor.logger.info("Fetching tickers from exchange: %s", exchangeName)
        if exchangeName == "Binance":
            df_all_tickers = pd.DataFrame(self.dataProcessor.ccxtBinance.fetchMarkets())
            df_spot_futures = df_all_tickers[((df_all_tickers["type"] == "spot") | (
                    (df_all_tickers["type"] == "swap") & df_all_tickers["linear"] == True)) & (
                                                     df_all_tickers["active"] == True)]
            if whiteListedQuoteAssets:
                with open("quoteAssetWhitelist.txt", "r") as f:
                    quoteAssets = f.read().split("\n")
                    df_spot_futures = df_spot_futures[df_spot_futures["quote"].isin(quoteAssets)]
            df_spot_futures["listingDatetime"] = None
            df_spot_futures = df_spot_futures[["id", "active", "swap", "margin", "listingDatetime", "base", "quote"]]
            df_spot_futures["exchangeName"] = "Binance"
            df_spot_futures.rename(columns={"id": "symbol",
                                            "active": "isActive",
                                            "swap": "isFutures",
                                            "margin": "hasMargin",
                                            "base": "baseAssetName",
                                            "quote": "quoteAssetName"}, inplace=True)
            df_spot_futures["isActive"] = df_spot_futures["isActive"].astype(int)
            df_spot_futures["isFutures"] = df_spot_futures["isFutures"].astype(int)
            df_spot_futures["hasMargin"] = df_spot_futures["hasMargin"].astype(int)
            return df_spot_futures
        else:
            self.dataProcessor.logger.error("Exchange not implemented: %s", exchangeName)
            raise NotImplementedError("Добавлен только Binance")

    def addTickers(self, dfNewTickers):
        self.dataProcessor.logger.info("Adding new tickers to database.")
        return self.dataProcessor._modify_query_pandas(dfNewTickers, "Ticker", if_exists='append', index=False)

class TickerGroup:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.dataProcessor.logger.info("Initializing TickerGroup class.")

    def addTickerGroup(self, tickerGroupName):
        self.dataProcessor.logger.info("Adding ticker group: %s", tickerGroupName)
        query = f"INSERT INTO TickerGroup (tickerGroupName) VALUES '{tickerGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def deleteTickerGroup(self, tickerGroupName):
        self.dataProcessor.logger.info("Deleting ticker group: %s", tickerGroupName)
        query = f"DELETE FROM TickerGroup WHERE tickerGroupName = '{tickerGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def renameTickerGroup(self, oldTickerGroupName, newTickerGroupName):
        self.dataProcessor.logger.info("Renaming ticker group from %s to %s", oldTickerGroupName, newTickerGroupName)
        query = f"UPDATE TickerGroup SET tickerGroupName = '{newTickerGroupName}' WHERE assetGroupName = '{oldTickerGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def getAllTickerGroups(self):
        self.dataProcessor.logger.info("Fetching all ticker groups.")
        query = f"SELECT * FROM TickerGroup;"
        return self.dataProcessor._select_pandas_query(query=query)

    def addTickersToTickerGroup(self, tickerGroupName, symbolIds):
        self.dataProcessor.logger.info("Adding tickers to ticker group: %s", tickerGroupName)
        tickerGroupId = self._getTickerGroupId(tickerGroupName)
        addedTickersCount = 0
        for symbolId in symbolIds:
            query = f"INSERT INTO TickerGroupLinkTable (tickerGroupId, symbolId) VALUES ({tickerGroupId}, {symbolId});"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                addedTickersCount += success
        self.dataProcessor.logger.info("Added %d tickers to ticker group: %s", addedTickersCount, tickerGroupName)
        return addedTickersCount

    def deleteTickersToTickerGroup(self, tickerGroupName, symbolIds):
        self.dataProcessor.logger.info("Deleting tickers from ticker group: %s", tickerGroupName)
        tickerGroupId = self._getTickerGroupId(tickerGroupName)
        deletedTickersCount = 0
        for symbolId in symbolIds:
            query = f"DELETE FROM TickerGroupLinkTable WHERE (tickerGroupId = {tickerGroupId} AND symbolId={symbolId});"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                deletedTickersCount += success
        self.dataProcessor.logger.info("Deleted %d tickers from ticker group: %s", deletedTickersCount, tickerGroupName)
        return deletedTickersCount

    def getTickersFromTickerGroup(self, tickerGroupName):
        self.dataProcessor.logger.info("Fetching tickers from ticker group: %s", tickerGroupName)
        tickerGroupId = self._getTickerGroupId(tickerGroupName)
        query = f"SELECT symbolId FROM TickerGroupLinkTable WHERE tickerGroupId={tickerGroupId};"
        return self.dataProcessor._select_pandas_query(query=query)

    def _getTickerGroupId(self, tickerGroupName):
        self.dataProcessor.logger.info("Fetching ticker group ID for: %s", tickerGroupName)
        query = f"SELECT tickerGroupId FROM TickerGroup WHERE tickerGroupName = '{tickerGroupName}';"
        return self.dataProcessor._basic_query(query=query, queryType="select")[0][0]

class Indicators:
    def __init__(self, dataProcessor):
        self.dataProcessor = dataProcessor
        self.dataProcessor.logger.info("Initializing Indicators class.")

    def getIndicatorRecords(self, indicatorTable, symbol, spot=True, timeframe="1m", lastRecord=False, fromDate=None, toDate=None):
        symbolId = self.dataProcessor._getSymbolId(
            symbol=symbol,
            spot=spot
        )
        if indicatorTable not in INDICATOR_TABLES:
            raise ValueError("Please provide a correct indicatorTable from: PriceOHLCV, OpenInterest, FundingRate, LongShortRatio, LiquidationAggregated")
        elif indicatorTable=="PriceOHLCV":
            self.dataProcessor.logger.info("Fetching %s for: %s, isSpot: %s", indicatorTable, symbol, spot)
        elif indicatorTable=="OpenInterest" \
                or indicatorTable=="FundingRate" \
                or indicatorTable=="LongShortRatio" \
                or indicatorTable=="LiquidationAggregated":
            self.dataProcessor.logger.info("Fetching %s for: %s", indicatorTable, symbol)

        params = ""
        if fromDate:
            params += f" AND timestamp >= {fromDate}"
        if toDate:
            params += f" AND timestamp <= {toDate}"
        if lastRecord:
            params += f" ORDER BY timestamp DESC LIMIT 1"
        params += ";"
        queryBase = f"SELECT * FROM {indicatorTable} WHERE symbolId={symbolId}"
        query = queryBase + params
        result = self.dataProcessor._select_pandas_query(query=query)
        if len(result) > 0:
            if indicatorTable=="PriceOHLCV":
                self.dataProcessor.logger.info("%s successfully fetched for %s, isSpot: %s", indicatorTable, symbol, spot)
            else:
                self.dataProcessor.logger.info("%s successfully fetched for %s", indicatorTable, symbol)
            return result
        else:
            if indicatorTable=="PriceOHLCV":
               self.dataProcessor.logger.info("Failed to fetch %s for %s, isSpot: %s", indicatorTable, symbol, spot)
            else:
                self.dataProcessor.logger.info("Failed to fetch %s for %s", indicatorTable, symbol)
            return None

    def _getNextDate(self, lastRecord):
        try:
            result = (lastRecord["timestamp"][0] + pd.Timedelta(days=1)).date()
            return result
        except Exception as e:
            self.dataProcessor.logger.info(f"Couldn't retrieve next date to update a table. \n Error: {e}")
            return None