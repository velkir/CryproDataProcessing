import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import ccxt
from playwright.sync_api import sync_playwright
import numpy as np
import queue
from queue import Queue, Empty
from threading import Thread
from sqlalchemy import event
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import sys
import warnings
import time
import random
from sqlalchemy.exc import InternalError
from mysql.connector.errors import InternalError as MySQLInternalError
import tempfile

class CryptoDataProcessor:
    def __init__(self, dbName):
        logger.info("Initializing CryptoDataProcessor with database: %s", dbName)
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
        logger.info("CryptoDataProcessor initialized successfully.")

    def _initialize_db_connection(self, dbName):
        logger.info("Initializing database connection to %s", dbName)
        connection_string = f"mysql+mysqlconnector://user:1234@localhost:3306/{dbName}"
        engine = create_engine(
            connection_string,
            echo=False,
            connect_args={
                'allow_local_infile': True  # Включаем загрузку локальных файлов на стороне клиента
            }
        )
        logger.info("Database connection established.")
        return engine

    def _basic_query(self, query, queryType="modify", params=None):
        thread_name = threading.current_thread().name
        logger.info(f"Thread {thread_name}: Executing basic query: {query}")
        try:
            with self._engine.connect() as con:
                result = con.execute(text(query), params)
                con.commit()
                if queryType == "modify":
                    logger.info(f"Thread {thread_name}: Query executed successfully, affected rows: {result.rowcount}")
                    return result.rowcount
                elif queryType == "select":
                    fetch_result = result.fetchall()
                    logger.info(f"Thread {thread_name}: Query executed successfully, fetched rows: {len(fetch_result)}")
                    return fetch_result
        except Exception as e:
            logger.exception(f"Thread {thread_name}: An error occurred during basic query execution: {e}")

    def _select_pandas_query(self, query, params=None, parse_dates=None, columns=None, chunksize=None):
        thread_name = threading.current_thread().name
        logger.info(f"Thread {thread_name}: Executing pandas select query: {query}")
        try:
            with self._engine.connect() as con:
                df = pd.read_sql(
                    sql=query,
                    con=con,
                    params=params,
                    parse_dates=parse_dates,
                    columns=columns,
                    chunksize=chunksize
                )
                logger.info(f"Thread {thread_name}: Pandas query executed successfully, fetched rows: {len(df)}")
                return df
        except Exception as e:
            logger.exception(f"Thread {thread_name}: An error occurred during pandas select query execution: {e}")

    import os
    import time
    import random
    from sqlalchemy.exc import InternalError
    from mysql.connector.errors import InternalError as MySQLInternalError

    def _modify_query_pandas(self, df, table, if_exists='append', index=False):
        thread_name = threading.current_thread().name
        logger.info(
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
                        logger.info(f"Thread {thread_name}: Replacing data in table {table}")
                        # Очищаем таблицу перед загрузкой данных
                        conn.execute(text(f"TRUNCATE TABLE `{table}`"))
                    elif if_exists == 'fail':
                        # Проверяем, есть ли данные в таблице
                        result = conn.execute(text(f"SELECT 1 FROM `{table}` LIMIT 1")).fetchone()
                        if result:
                            logger.error(
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
                logger.info(f"Thread {thread_name}: Data loaded successfully using LOAD DATA INFILE.")
                break  # Если успешно, выходим из цикла
            except (InternalError, MySQLInternalError) as e:
                if '1213' in str(e):
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.error(f"Thread {thread_name}: Max retries exceeded. Could not resolve deadlock.")
                        # Удаляем временный CSV-файл
                        os.remove(temp_csv_file)
                        raise
                    wait_time = random.uniform(0.5, 2.0)
                    logger.warning(
                        f"Thread {thread_name}: Deadlock detected. Retrying {retry_count}/{max_retries} after {wait_time:.2f} seconds.")
                    time.sleep(wait_time)
                else:
                    logger.exception(
                        f"Thread {thread_name}: An error occurred during optimized modify query execution: {e}")
                    # Удаляем временный CSV-файл
                    os.remove(temp_csv_file)
                    raise
            except Exception as e:
                logger.exception(f"Thread {thread_name}: An unexpected error occurred: {e}")
                # Удаляем временный CSV-файл
                os.remove(temp_csv_file)
                raise

    # def _modify_query_pandas(self, df, table, if_exists='append', index=False, method="multi", chunksize=50000):
    #     thread_name = threading.current_thread().name
    #     logger.info(f"Thread {thread_name}: Executing pandas modify query on table {table}")
    #     try:
    #         with self._engine.connect() as conn:
    #             result = df.to_sql(
    #                 name=table,
    #                 con=conn,
    #                 if_exists=if_exists,
    #                 index=index,
    #                 method=method,
    #                 chunksize=chunksize
    #             )
    #             logger.info(f"Thread {thread_name}: Pandas modify query executed successfully.")
    #             return result
    #     except Exception as e:
    #         logger.exception(f"Thread {thread_name}: An error occurred during pandas modify query execution: {e}")

    def _register_event(self):
        logger.info("Registering database event for fast executemany.")
        @event.listens_for(self._engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True
        logger.info("Database event registered.")

class Exchange:
    def __init__(self, dataProcessor):
        logger.info("Initializing Exchange class.")
        self.dataProcessor = dataProcessor

    def add_exchange(self, exchangeName):
        logger.info("Adding exchange: %s", exchangeName)
        query = f"INSERT INTO Exchange (exchangeName) VALUES ('{exchangeName}');"
        return self.dataProcessor._basic_query(query)

    def delete_exchange(self, exchangeName):
        logger.info("Deleting exchange: %s", exchangeName)
        query = f"DELETE FROM Exchange WHERE exchangeName='{exchangeName}';"
        return self.dataProcessor._basic_query(query)

    def getExchange(self, exchangeName):
        logger.info("Fetching exchange: %s", exchangeName)
        query = f"SELECT * FROM Exchange WHERE exchangeName = '{exchangeName}';"
        result = self.dataProcessor._select_pandas_query(query=query)
        if len(result) > 0:
            logger.info("Exchange found: %s", exchangeName)
            return result
        else:
            logger.info("Exchange not found: %s", exchangeName)
            return None

    def getAllExchanges(self, params=None, parse_dates=None, columns=None, chunksize=None):
        logger.info("Fetching all exchanges.")
        query = f"SELECT * FROM Exchange;"
        return self.dataProcessor._select_pandas_query(query=query,
                                                       params=params,
                                                       parse_dates=parse_dates,
                                                       columns=columns,
                                                       chunksize=chunksize)

class ExchangeBalanceHistory:
    def __init__(self, dataProcessor):
        logger.info("Initializing ExchangeBalanceHistory class.")
        self.dataProcessor = dataProcessor

    def updateExchangeBalance(self, exchangeName):
        logger.info("Updating exchange balance for: %s", exchangeName)
        # Логика запроса к coinglass  https://docs.coinglass.com/reference/exchange-balance-list
        # Логика обработки данных в pd.df и добавление в БД
        pass

    def getExchangeBalanceHistoryRecords(self, exchangeName, fromDatetime=None):
        logger.info("Fetching exchange balance history records for: %s", exchangeName)
        base_query = f"SELECT * FROM ExchangeBalanceHistory WHERE exchangeName='{exchangeName}'"
        params = ""
        closing_part = " ORDER BY timestamp;"
        if fromDatetime is not None:
            params = f" AND timestamp >= '{fromDatetime}'"
        query = base_query + params + closing_part
        return self.dataProcessor._select_pandas_query(query=query)

    def getLastExchangeBalanceHistoryRecord(self, exchangeName):
        logger.info("Fetching last exchange balance history record for: %s", exchangeName)
        query = f"SELECT balance FROM ExchangeBalanceHistory WHERE exchangeName='{exchangeName}' ORDER BY timestamp DESC LIMIT 1;"
        result = self.dataProcessor._basic_query(query=query, queryType="select")
        if result:
            logger.info("Last balance record found for exchange: %s", exchangeName)
            return result[0][0]
        else:
            logger.info("No balance record found for exchange: %s", exchangeName)
            return None

class Asset:
    def __init__(self, dataProcessor):
        logger.info("Initializing Asset class.")
        self.dataProcessor = dataProcessor
        self.binance_assets_url = "https://www.binance.com/bapi/apex/v1/friendly/apex/marketing/complianceSymbolList"

    def _getAssetsFromDB(self):
        logger.info("Fetching assets from database.")
        query = f"SELECT * FROM Asset ORDER BY cmcId"
        return self.dataProcessor._select_pandas_query(query=query)

    def _getAssetsFromExchange(self, exchangeName="Binance", assetHistory=False):
        logger.info("Fetching assets from exchange: %s", exchangeName)
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
                logger.error("Exchange not implemented: %s", exchangeName)
                raise NotImplementedError("Добавлен только Binance")
        except Exception as e:
            logger.exception(f"An error occurred while fetching assets from exchange: {e}")

    def _makeRequestAssets(self):
        logger.info("Making request to Binance assets URL.")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            page.goto('https://www.binance.com')
            page.wait_for_load_state('networkidle')
            response = page.request.get(self.binance_assets_url)
            data = response.json()
            browser.close()
            logger.info("Received data from Binance assets URL.")
            return data

    def updateAssets(self, exchangeName="Binance", DBAssets=None, exchangeAssets=None):
        logger.info("Updating assets from exchange: %s", exchangeName)
        if exchangeAssets is None:
            exchangeAssets = self._getAssetsFromExchange(exchangeName)
        if DBAssets is None:
            DBAssets = self._getAssetsFromDB()
        newAssets = exchangeAssets[~exchangeAssets["cmcId"].isin(DBAssets["cmcId"])]
        newAssets_without_tags = newAssets.drop(columns="tag")
        if not newAssets.empty:
            logger.info("New assets found, adding to database.")
            assets_affected_rows = self.dataProcessor._modify_query_pandas(newAssets_without_tags, "Asset",
                                                                           if_exists='append', index=False, method="multi")
            df_tags = newAssets[["assetName", "tag"]].explode("tag")
            df_tags.dropna(inplace=True)
            tags_affected_rows = self.dataProcessor.assetTag.addTags(df_tags)
            logger.info("Assets updated successfully.")
            return assets_affected_rows, tags_affected_rows
        else:
            logger.info("No new assets to update.")
            return None

class AssetTag:
    def __init__(self, dataProcessor):
        logger.info("Initializing AssetTag class.")
        self.dataProcessor = dataProcessor

    def addTags(self, df_tags):
        logger.info("Adding asset tags to database.")
        return self.dataProcessor._modify_query_pandas(df_tags, "AssetTag", if_exists='append', index=False,
                                                       method="multi")

    def getTags(self, assetsList=None):
        logger.info("Fetching asset tags.")
        query = f"SELECT * FROM AssetTag"
        params = ";"
        if assetsList:
            params = f" WHERE {''.join(['symbol = ' + '\'' + str(asset) + '\' OR ' for asset in assetsList])}"
            params = params[:-4] + ";"
        query += params
        return self.dataProcessor._select_pandas_query(query)

class AssetGroup:
    def __init__(self, dataProcessor):
        logger.info("Initializing AssetGroup class.")
        self.dataProcessor = dataProcessor

    def addAssetGroup(self, assetGroupName):
        logger.info("Adding asset group: %s", assetGroupName)
        query = f"INSERT INTO AssetGroup (assetGroupName) VALUES ('{assetGroupName}');"
        return self.dataProcessor._basic_query(query=query)

    def deleteAssetGroup(self, assetGroupName):
        logger.info("Deleting asset group: %s", assetGroupName)
        query = f"DELETE FROM AssetGroup WHERE assetGroupName = '{assetGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def renameAssetGroup(self, oldAssetGroupName, newAssetGroupName):
        logger.info("Renaming asset group from %s to %s", oldAssetGroupName, newAssetGroupName)
        query = f"UPDATE AssetGroup SET assetGroupName = '{newAssetGroupName}' WHERE assetGroupName = '{oldAssetGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def getAllAssetGroups(self):
        logger.info("Fetching all asset groups.")
        query = f"SELECT * FROM AssetGroup;"
        return self.dataProcessor._select_pandas_query(query=query)

    def addAssetsToAssetGroup(self, assetGroupName, assetsList):
        logger.info("Adding assets to asset group: %s", assetGroupName)
        assetGroupId = self._getAssetGroupId(assetGroupName)
        addedAssetsCount = 0
        for asset in assetsList:
            query = f"INSERT INTO AssetGroupLinkTable (assetGroupId, assetName) VALUES ({assetGroupId}, '{asset}');"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                addedAssetsCount += success
        logger.info("Added %d assets to asset group: %s", addedAssetsCount, assetGroupName)
        return addedAssetsCount

    def deleteAssetsFromAssetGroup(self, assetGroupName, assetsList):
        logger.info("Deleting assets from asset group: %s", assetGroupName)
        assetGroupId = self._getAssetGroupId(assetGroupName)
        deletedAssetsCount = 0
        for asset in assetsList:
            query = f"DELETE FROM AssetGroupLinkTable WHERE (assetGroupId={assetGroupId} AND assetName='{asset}');"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                deletedAssetsCount += success
        logger.info("Deleted %d assets from asset group: %s", deletedAssetsCount, assetGroupName)
        return deletedAssetsCount

    def getAssetsFromAssetGroup(self, assetGroupName):
        logger.info("Fetching assets from asset group: %s", assetGroupName)
        assetGroupId = self._getAssetGroupId(assetGroupName)
        query = f"SELECT assetName FROM assetGroupLinkTable WHERE assetGroupId = {assetGroupId};"
        return self.dataProcessor._select_pandas_query(query=query)

    def _getAssetGroupId(self, assetGroupName):
        logger.info("Fetching asset group ID for: %s", assetGroupName)
        query = f"SELECT assetGroupId FROM AssetGroup WHERE assetGroupName = '{assetGroupName}';"
        return self.dataProcessor._basic_query(query=query, queryType="select")[0][0]

class AssetHistory:
    def __init__(self, dataProcessor):
        logger.info("Initializing AssetHistory class.")
        self.dataProcessor = dataProcessor

    def addAssetsHistoryRecords(self):
        logger.info("Adding asset history records.")
        df = self.dataProcessor.asset._getAssetsFromExchange(assetHistory=True)
        return self.dataProcessor._modify_query_pandas(df, "AssetHistoryRecords")

    def getAssetHistoryRecords(self, assetName, fromDatetime=None):
        logger.info("Fetching asset history records for: %s", assetName)
        base_query = f"SELECT * FROM AssetHistory WHERE assetName = '{assetName}'"
        if fromDatetime:
            params = f" AND Timestamp >= '{fromDatetime}';"
        else:
            params = ";"
        query = base_query + params
        result = self.dataProcessor._select_pandas_query(query=query)
        if len(result) > 0:
            logger.info("Asset history records found for: %s", assetName)
            return result
        else:
            logger.info("No asset history records found for: %s", assetName)
            return None

class Ticker:
    def __init__(self, dataProcessor):
        logger.info("Initializing Ticker class.")
        self.dataProcessor = dataProcessor

    def getTickersFromDB(self, symbolsList=None, params=None):
        logger.info("Fetching tickers from database.")
        query = f"SELECT * FROM Ticker"
        params = ";"
        if symbolsList:
            params = f" WHERE {''.join(['symbol = ' + '\'' + str(symbol) + '\' OR ' for symbol in symbolsList])}"
            params = params[:-4] + ";"
        query += params
        return self.dataProcessor._select_pandas_query(query)

    def getSymbolId(self, symbol, exchangeName="Binance", spot=True):
        logger.info("Fetching symbol ID for symbol: %s", symbol)
        if spot:
            isFutures = 0
        else:
            isFutures = 1
        query = f"SELECT symbolId FROM Ticker WHERE symbol='{symbol}' AND exchangeName='{exchangeName}' AND isFutures={isFutures};"
        result = self.dataProcessor._basic_query(query, queryType="select")
        if result:
            logger.info("Symbol ID found: %s for symbol: %s", result[0][0], symbol)
            return result
        else:
            logger.info("Symbol ID not found for symbol: %s", symbol)
            return None

    def updateTickers(self):
        logger.info("Updating tickers.")
        df_exchange_tickers = self.getTickersFromExchange()
        df_db_tickers = self.getTickersFromDB()
        df_db_tickers.drop(columns=["symbolId"], inplace=True)
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
        if not newTickers.empty:
            logger.info("New tickers found, adding to database.")
            return self.addTickers(newTickers)
        else:
            logger.info("No new tickers to update.")
            return None

    def getTickersFromExchange(self, exchangeName="Binance", whiteListedQuoteAssets=True):
        logger.info("Fetching tickers from exchange: %s", exchangeName)
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
            return df_spot_futures
        else:
            logger.error("Exchange not implemented: %s", exchangeName)
            raise NotImplementedError("Добавлен только Binance")

    def addTickers(self, dfNewTickers):
        logger.info("Adding new tickers to database.")
        return self.dataProcessor._modify_query_pandas(dfNewTickers, "Ticker", if_exists='append', index=False,
                                                       method="multi", chunksize=50)

class TickerGroup:
    def __init__(self, dataProcessor):
        logger.info("Initializing TickerGroup class.")
        self.dataProcessor = dataProcessor

    def addTickerGroup(self, tickerGroupName):
        logger.info("Adding ticker group: %s", tickerGroupName)
        query = f"INSERT INTO TickerGroup (tickerGroupName) VALUES '{tickerGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def deleteTickerGroup(self, tickerGroupName):
        logger.info("Deleting ticker group: %s", tickerGroupName)
        query = f"DELETE FROM TickerGroup WHERE tickerGroupName = '{tickerGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def renameTickerGroup(self, oldTickerGroupName, newTickerGroupName):
        logger.info("Renaming ticker group from %s to %s", oldTickerGroupName, newTickerGroupName)
        query = f"UPDATE TickerGroup SET tickerGroupName = '{newTickerGroupName}' WHERE assetGroupName = '{oldTickerGroupName}';"
        return self.dataProcessor._basic_query(query=query)

    def getAllTickerGroups(self):
        logger.info("Fetching all ticker groups.")
        query = f"SELECT * FROM TickerGroup;"
        return self.dataProcessor._select_pandas_query(query=query)

    def addTickersToTickerGroup(self, tickerGroupName, symbolIds):
        logger.info("Adding tickers to ticker group: %s", tickerGroupName)
        tickerGroupId = self._getTickerGroupId(tickerGroupName)
        addedTickersCount = 0
        for symbolId in symbolIds:
            query = f"INSERT INTO TickerGroupLinkTable (tickerGroupId, symbolId) VALUES ({tickerGroupId}, {symbolId});"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                addedTickersCount += success
        logger.info("Added %d tickers to ticker group: %s", addedTickersCount, tickerGroupName)
        return addedTickersCount

    def deleteTickersToTickerGroup(self, tickerGroupName, symbolIds):
        logger.info("Deleting tickers from ticker group: %s", tickerGroupName)
        tickerGroupId = self._getTickerGroupId(tickerGroupName)
        deletedTickersCount = 0
        for symbolId in symbolIds:
            query = f"DELETE FROM TickerGroupLinkTable WHERE (tickerGroupId = {tickerGroupId} AND symbolId={symbolId});"
            success = self.dataProcessor._basic_query(query=query)
            if success:
                deletedTickersCount += success
        logger.info("Deleted %d tickers from ticker group: %s", deletedTickersCount, tickerGroupName)
        return deletedTickersCount

    def getTickersFromTickerGroup(self, tickerGroupName):
        logger.info("Fetching tickers from ticker group: %s", tickerGroupName)
        tickerGroupId = self._getTickerGroupId(tickerGroupName)
        query = f"SELECT symbolId FROM TickerGroupLinkTable WHERE tickerGroupId={tickerGroupId};"
        return self.dataProcessor._select_pandas_query(query=query)

    def _getTickerGroupId(self, tickerGroupName):
        logger.info("Fetching ticker group ID for: %s", tickerGroupName)
        query = f"SELECT tickerGroupId FROM TickerGroup WHERE tickerGroupName = '{tickerGroupName}';"
        return self.dataProcessor._basic_query(query=query, queryType="select")[0][0]

class CSVMerger:
    def __init__(self, dataProcessor):
        logger.info("Initializing CSVMerger.")
        self.dataProcessor = dataProcessor
        self.basicPath = os.getcwd()
        self.shared_lock = threading.Lock()
        self.priceOHLCVSpotFolder = "data/spot/klines"
        self.priceOHLCVUmFuturesFolder = "data/futures/um/klines"
        self.fundingRatesFolder = "data/futures/um/fundingRate"
        self.metricsFolder = "data/futures/um/metrics"
        logger.info("CSVMerger initialized successfully.")

    def mergePriceOHLCV(self, spotTickers=None, UMFuturesTickers=None, timeframe="1m", fromDateTime=None):
        spotPath = os.path.join(self.basicPath, self.priceOHLCVSpotFolder)
        umFuturesPath = os.path.join(self.basicPath, self.priceOHLCVUmFuturesFolder)
        tasks = []

        logger.info(f"Starting mergePriceOHLCV with timeframe: {timeframe} and fromDateTime: {fromDateTime}")

        if spotTickers:
            downloadedSpotTickers = os.listdir(spotPath)
            logger.info(f"Found {len(downloadedSpotTickers)} spot tickers in directory: {spotPath}")

            tasks.extend(
                [(spotTicker, spotPath, 1) for spotTicker in spotTickers if spotTicker in downloadedSpotTickers])

        if UMFuturesTickers:
            downloadedFuturesTickers = os.listdir(umFuturesPath)
            logger.info(f"Found {len(downloadedFuturesTickers)} UM Futures tickers in directory: {umFuturesPath}")

            tasks.extend([(umFuturesTicker, umFuturesPath, 0) for umFuturesTicker in UMFuturesTickers if
                          umFuturesTicker in downloadedFuturesTickers])

        numThreads = 30
        logger.info(f"Starting {numThreads} threads to process the tasks")

        with ThreadPoolExecutor(max_workers=numThreads) as executor:
            futures = {executor.submit(self._merge1PriceOHLCV, task, fromDateTime, timeframe): task for task in tasks}

            for future in as_completed(futures):
                task = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.exception(f"An error occurred processing task {task}: {e}")

        logger.info("Completed merging OHLCV data.")

    def _merge1PriceOHLCV(self, task, fromDateTime=None, timeframe="1m"):
        ticker, tickerPath, isSpot = task
        thread_name = threading.current_thread().name
        logger.info(f"Thread {thread_name}: Processing OHLCV for ticker: {ticker}, isSpot: {isSpot}")

        try:
            with self.shared_lock:
                symbolId = self._getSymbolId(symbol=ticker, isSpot=isSpot)
            logger.info(f"Thread {thread_name}: Obtained symbolId: {symbolId} for ticker: {ticker}")
            tickerPath = os.path.join(tickerPath, ticker, timeframe)
            tickerFiles = [file for file in os.listdir(tickerPath) if not file.startswith(".")]
            logger.info(f"Thread {thread_name}: Found {len(tickerFiles)} files for ticker: {ticker}")
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
                logger.info(f"Thread {thread_name}: Reading file: {tickerFilePath}")
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
            logger.info(f"Thread {thread_name}: Data processed and stored for ticker: {ticker}, isSpot: {isSpot}")
        except Exception as e:
            logger.exception(f"Thread {thread_name}: Error while entering data into the database for ticker: {ticker}, isSpot: {isSpot}")

    def _getSymbolId(self, symbol, isSpot):
        logger.info(f"Fetching symbol ID for symbol: {symbol}, isSpot: {isSpot}")
        result = self.dataProcessor.ticker.getSymbolId(symbol=symbol, spot=isSpot)
        if result:
            return result[0][0]
        else:
            logger.error(f"Symbol ID not found for symbol: {symbol}, isSpot: {isSpot}")
            raise ValueError(f"Symbol ID not found for symbol: {symbol}, isSpot: {isSpot}")

    def mergeFundingRates(self, UMFuturesTickers, fromDateTime=None):
        fundingRateFullPath = os.path.join(self.basicPath, self.fundingRatesFolder)
        tasks = []

        logger.info(f"Starting mergeFundingRates with fromDateTime: {fromDateTime}")

        downloadedFuturesTickers = os.listdir(fundingRateFullPath)
        logger.info(f"Found {len(downloadedFuturesTickers)} funding rate tickers in directory: {fundingRateFullPath}")

        tasks.extend([ticker for ticker in UMFuturesTickers if ticker in downloadedFuturesTickers])

        numThreads = 30
        logger.info(f"Starting {numThreads} threads to process the funding rates")

        with ThreadPoolExecutor(max_workers=numThreads) as executor:
            futures = {executor.submit(self._merge1FundingRate, ticker, fundingRateFullPath, fromDateTime): ticker for ticker in tasks}

            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.exception(f"An error occurred processing funding rates for ticker {ticker}: {e}")

        logger.info("Completed merging funding rates.")

    def _merge1FundingRate(self, ticker, fundingRateFullPath, fromDateTime):
        thread_name = threading.current_thread().name
        logger.info(f"Thread {thread_name}: Processing funding rate for ticker: {ticker}")

        try:
            with self.shared_lock:
                symbolId = self._getSymbolId(symbol=ticker, isSpot=0)
            logger.info(f"Thread {thread_name}: Obtained symbolId: {symbolId} for ticker (futures): {ticker}")
            tickerPath = os.path.join(fundingRateFullPath, ticker)
            tickerFiles = [file for file in os.listdir(tickerPath) if not file.startswith(".")]
            logger.info(f"Thread {thread_name}: Found {len(tickerFiles)} files for ticker: {ticker}")
            dfTicker = pd.DataFrame(columns=[
                "symbolId",
                "timestamp",
                "fundingRate",
                "fundingIntervalHours"
            ])
            for tickerFile in tickerFiles:
                tickerFilePath = os.path.join(tickerPath, tickerFile)
                logger.info(f"Thread {thread_name}: Reading file: {tickerFilePath}")
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
            logger.info(f"Thread {thread_name}: Data processed and stored for funding rates ticker: {ticker}")
        except Exception as e:
            logger.exception(f"Thread {thread_name}: Error while processing funding rates for ticker: {ticker}")

    def mergeMetrics(self, UMFuturesTickers, fromDateTime=None):
        metricsFullPath = os.path.join(self.basicPath, self.metricsFolder)
        tasks = []

        logger.info(f"Starting mergeMetrics with fromDateTime: {fromDateTime}")

        downloadedFuturesTickers = os.listdir(metricsFullPath)
        logger.info(f"Found {len(downloadedFuturesTickers)} metrics tickers in directory: {metricsFullPath}")

        tasks.extend([ticker for ticker in UMFuturesTickers if ticker in downloadedFuturesTickers])

        numThreads = 30
        logger.info(f"Starting {numThreads} threads to process the metrics")

        with ThreadPoolExecutor(max_workers=numThreads) as executor:
            futures = {executor.submit(self._merge1Metrics, ticker, metricsFullPath, fromDateTime): ticker for ticker in tasks}

            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.exception(f"An error occurred processing metrics for ticker {ticker}: {e}")

        logger.info("Completed merging metrics.")

    def _merge1Metrics(self, ticker, metricsFullPath, fromDateTime, timeframe="5m"):
        thread_name = threading.current_thread().name
        logger.info(f"Thread {thread_name}: Processing metrics for ticker: {ticker}")

        try:
            with self.shared_lock:
                symbolId = self._getSymbolId(symbol=ticker, isSpot=0)
            logger.info(f"Thread {thread_name}: Obtained symbolId: {symbolId} for ticker (futures): {ticker}")
            tickerPath = os.path.join(metricsFullPath, ticker)
            tickerFiles = [file for file in os.listdir(tickerPath) if not file.startswith(".")]
            logger.info(f"Thread {thread_name}: Found {len(tickerFiles)} files for ticker: {ticker}")

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
                logger.info(f"Thread {thread_name}: Reading file: {tickerFilePath}")
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
            logger.info(f"Thread {thread_name}: Data processed and stored for metrics ticker: {ticker}")
        except Exception as e:
            logger.exception(f"Thread {thread_name}: Error while processing metrics for ticker: {ticker}")

def loadTickersFromFile(path):
    logger.info(f"Loading tickers from file '{path}'")
    with open(path, "r") as f:
        tickers = f.readlines()
        final_tickers = [ticker.strip("'\n") for ticker in tickers]
        logger.info(f"Tickers loaded: {final_tickers}")
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

file_handler = logging.FileHandler('CryptoDataProcessor.log', mode='a', encoding='utf-8')
file_handler.setLevel(logging.INFO)

# Create formatter with timestamp
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Set formatter for handlers
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

pd.set_option("display.max_rows", 150)
pd.set_option("display.max_columns", 150)
pd.set_option("max_colwidth", 1000)

dataProcessor = CryptoDataProcessor("test")
# dataProcessor.asset.updateAssets()
# dataProcessor.ticker.updateTickers()

spotTickers = loadTickersFromFile("spotTickers.txt")
umFuturesTickers = loadTickersFromFile("umFuturesTickers.txt")

csvMerger = CSVMerger(dataProcessor=dataProcessor)
#
from datetime import datetime
timestart = datetime.now()

csvMerger.mergePriceOHLCV(
    # spotTickers=['AERGOUSDT', 'AEURUSDT', 'AEVOUSDT', 'AGLDUSDT', 'AIUSDT', 'AKROUSDT', 'ALCXUSDT', 'ALGOUSDT',
    #              'ALICEUSDT', 'ALPACAUSDT', 'ALPHAUSDT', 'ALPINEUSDT', 'ALTUSDT', 'AMBUSDT', 'AMPUSDT'],
        spotTickers=["BTCUSDT", "ETHUSDT"],
        UMFuturesTickers=["BTCUSDT", "ETHUSDT"],
    # spotTickers=spotTickers,
    # UMFuturesTickers=umFuturesTickers,
    timeframe="1m")
# csvMerger.mergeMetrics(
#     UMFuturesTickers=["DOGEUSDT", "EOSUSDT", "TRXUSDT", "1000PEPEUSDT", "1000SHIBUSDT", "ETHUSDT"]
#     # UMFuturesTickers=umFuturesTickers
#                         )
# csvMerger.mergeFundingRates(
#     UMFuturesTickers=["DOGEUSDT", "EOSUSDT", "TRXUSDT", "1000PEPEUSDT", "1000SHIBUSDT", "ETHUSDT"])
# timeend = datetime.now()
# print(f"Total time: {timeend-timestart}")


#AssetGroup
# dataProcessor.assetGroup.addAssetGroup("test1")
# dataProcessor.assetGroup.addAssetGroup("test2")
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

