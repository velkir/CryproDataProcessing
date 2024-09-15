def create_use_database(databaseName):
    return f"CREATE DATABASE {databaseName}; USE {databaseName};"


def create_tables():
    return [
    """
    CREATE TABLE Exchange (
    exchangeId TINYINT AUTO_INCREMENT PRIMARY KEY,
    exchangeName VARCHAR(30) UNIQUE NOT NULL
);

CREATE TABLE ExchangeBalanceHistory (
    exchangeBalanceRecordId BIGINT AUTO_INCREMENT UNIQUE,
    exchangeName VARCHAR(30),
    timestamp DATETIME,
    balance BIGINT,
    PRIMARY KEY (exchangeName, timestamp),
    FOREIGN KEY (exchangeName) REFERENCES Exchange(exchangeName),
    CHECK (balance IS NULL OR (balance >= 0 AND balance <= 1e14))
);

CREATE TABLE Asset (
    cmcId MEDIUMINT PRIMARY KEY,
    assetName VARCHAR(30) UNIQUE
);

CREATE TABLE AssetGroup (
    assetGroupId SMALLINT AUTO_INCREMENT PRIMARY KEY,
    assetGroupName VARCHAR(100) UNIQUE,
    lastUpdate DATETIME
);

CREATE TABLE AssetGroupLinkTable (
    assetName VARCHAR(30),
    assetGroupId SMALLINT,
    PRIMARY KEY (assetName, assetGroupId),
    FOREIGN KEY (assetGroupId) REFERENCES AssetGroup(assetGroupId),
    FOREIGN KEY (assetName) REFERENCES Asset(assetName)
);

CREATE TABLE AssetHistory (
    assetHistoryRecordId BIGINT AUTO_INCREMENT UNIQUE,
    assetName VARCHAR(30),
    timestamp DATETIME,
    cmcRank MEDIUMINT,
    marketcap BIGINT,
    circulatingSupply BIGINT,
    maxSupply BIGINT,
    totalSupply BIGINT,
    PRIMARY KEY (assetName, timestamp),
    FOREIGN KEY (assetName) REFERENCES Asset(assetName),
    CHECK (cmcRank > 0 AND cmcRank < 1e9),
    CHECK (
        (marketcap IS NULL OR (marketcap >= 0 AND marketcap <= 1e16)) AND
        (circulatingSupply IS NULL OR (circulatingSupply >= 0 AND circulatingSupply <= 1e16)) AND
        (maxSupply IS NULL OR (maxSupply >= 0 AND maxSupply <= 1e16)) AND
        (totalSupply IS NULL OR (totalSupply >= 0 AND totalSupply <= 1e16))
    )
);

CREATE TABLE Ticker (
    symbolId SMALLINT AUTO_INCREMENT UNIQUE,
    exchangeName VARCHAR(30),
    symbol VARCHAR(30),
    isFutures BOOL NOT NULL DEFAULT 0,
    hasMargin BOOL NOT NULL DEFAULT 0,
    pair VARCHAR(30),
    status VARCHAR(30) DEFAULT 'Trading',
    listingDate DATETIME,
    onboardDate DATETIME,
    contractType VARCHAR(30),
    baseAssetName VARCHAR(30),
    quoteAssetName VARCHAR(30),
    underlyingType VARCHAR(30),
    PRIMARY KEY (symbol, exchangeName),
    FOREIGN KEY (exchangeName) REFERENCES Exchange(exchangeName),
    FOREIGN KEY (baseAssetName) REFERENCES Asset(assetName),
    FOREIGN KEY (quoteAssetName) REFERENCES Asset(assetName)
);

CREATE TABLE PriceOHLCV (
    priceOhlcvRecordId BIGINT UNIQUE AUTO_INCREMENT ,
    symbolId SMALLINT,
    timeframe VARCHAR(10),
    timestamp DATETIME,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    PRIMARY KEY (symbolId, timeframe, timestamp),
    FOREIGN KEY (symbolId) REFERENCES Ticker(symbolId),
    CHECK (open >= 0 AND open <= 1e6),
    CHECK (high >= 0 AND high <= 1e6),
    CHECK (low >= 0 AND low <= 1e6),
    CHECK (close >= 0 AND close <= 1e6),
    CHECK (volume >= 0 AND volume <= 1e16)
);

CREATE TABLE LongShortRatio (
    longShortRatioRecordId BIGINT AUTO_INCREMENT UNIQUE,
    symbolId SMALLINT,
    timeframe VARCHAR(10),
    timestamp DATETIME,
    topTradersAccountsRatio FLOAT,
    topTradersPositionsRatio FLOAT,
    takerVolumeRatio FLOAT,
    PRIMARY KEY (symbolId, timeframe, timestamp),
    FOREIGN KEY (symbolId) REFERENCES Ticker(symbolId),
    CHECK (topTradersAccountsRatio >= 0 AND topTradersAccountsRatio <= 100000),
    CHECK (topTradersPositionsRatio >= 0 AND topTradersPositionsRatio <= 100000),
    CHECK (takerVolumeRatio >= 0 AND takerVolumeRatio <= 1e15)
);

CREATE TABLE OpenInterest (
    openInterestOhlcRecordId BIGINT AUTO_INCREMENT UNIQUE,
    symbolId SMALLINT,
    timeframe VARCHAR(10),
    timestamp DATETIME,
    openInterestUsd BIGINT,
    openInterestAsset BIGINT,
    PRIMARY KEY (symbolId, timeframe, timestamp),
    FOREIGN KEY (symbolId) REFERENCES Ticker(symbolId),
    CHECK (openInterestUsd >= 0 AND openInterestUsd <= 1e15),
    CHECK (openInterestAsset >= 0 AND openInterestAsset <= 1e15)
);

CREATE TABLE LiquidationAggregated (
    liquidationAggregatedRecordId BIGINT AUTO_INCREMENT UNIQUE,
    symbolId SMALLINT,
    timeframe VARCHAR(10),
    timestamp DATETIME,
    longLiquidations BIGINT,
    shortLiquidations BIGINT,
    PRIMARY KEY (symbolId, timeframe, timestamp),
    FOREIGN KEY (symbolId) REFERENCES Ticker(symbolId),
    CHECK (longLiquidations >= 0 AND longLiquidations <= 1e12),
    CHECK (shortLiquidations >= 0 AND shortLiquidations <= 1e12)
);

CREATE TABLE FundingRate (
    fundingRateOhlcId BIGINT AUTO_INCREMENT UNIQUE,
    symbolId SMALLINT,
    timestamp DATETIME,
    fundingRate FLOAT,
    fundingIntervalHours TINYINT,
    PRIMARY KEY (symbolId, timestamp),
    FOREIGN KEY (symbolId) REFERENCES Ticker(symbolId),
    CHECK (fundingRate >= -100 AND fundingRate <= 100),
    CHECK (fundingIntervalHours > 0 AND fundingIntervalHours <= 48)
);

CREATE TABLE FearGreed (
    fearGreedRecordId BIGINT AUTO_INCREMENT UNIQUE,
    fearGreedValue FLOAT,
    btcPrice BIGINT,
    timestamp DATETIME PRIMARY KEY,
    CHECK (fearGreedValue >= 0 AND fearGreedValue <= 1e12),
    CHECK (btcPrice >= 0 AND btcPrice <= 1e12)
);

CREATE TABLE TickerGroup (
    tickerGroupId SMALLINT AUTO_INCREMENT PRIMARY KEY,
    tickerGroupName VARCHAR(100) UNIQUE,
    lastUpdate DATETIME
);

CREATE TABLE TickerGroupLinkTable (
    symbolId SMALLINT,
    tickerGroupId SMALLINT,
    PRIMARY KEY (symbolId, tickerGroupId),
    FOREIGN KEY (symbolId) REFERENCES Ticker(symbolId),
    FOREIGN KEY (tickerGroupId) REFERENCES TickerGroup(tickerGroupId)
);
    """
]
