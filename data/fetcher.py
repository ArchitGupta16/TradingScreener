"""
Data fetcher module for yfinance integration.
Handles stock data retrieval and market data normalization.
"""
import yfinance as yf
import pandas as pd
import numpy as np
from typing import List, Optional
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv
from breeze_connect import BreezeConnect
import time
import pickle
from data.postgres_store import PostgresStore

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

class BreezeFetcher:
    """
    Fetches stock data from Breeze (Broking platform) API.
    Handles authentication and data retrieval.
    """
    
    def __init__(self):
        """
        Initialize Breeze Fetcher with API credentials.
        
        Args:
            api_key: Breeze API key (defaults to BREEZE_API_KEY env var)
            api_secret: Breeze API secret (defaults to BREEZE_API_SECRET env var)
        """
        self.api_key = os.getenv('BREEZE_API_KEY')
        self.api_secret = os.getenv('BREEZE_API_SECRET')
        self.session_token = os.getenv('BREEZE_SESSION_TOKEN')
        
        if not self.api_key or not self.api_secret:
            raise ValueError(
                "Breeze API credentials not provided. "
                "Set BREEZE_API_KEY and BREEZE_API_SECRET environment variables or pass them as arguments."
            )
        
        try:
            self.breeze = BreezeConnect(api_key=self.api_key)
            self.breeze.generate_session(api_secret=self.api_secret, session_token=self.session_token)
            logger.info("Breeze API session initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Breeze API: {e}")
            raise
        
        self.cache = {}
        
        # Initialize PostgreSQL store
        self.postgres_store = PostgresStore(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 5432)),
            database=os.getenv('DB_NAME', 'history'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password')
        )
        # Attempt to connect to database
        if self.postgres_store.connect():
            self.postgres_store.create_equity_table()
        else:
            logger.warning("PostgreSQL connection failed. Data will not be stored to database.")
    
    def get_historical_data(
        self, 
        symbols: List[str], 
        period: str = "3mo",
        interval: str = "1day"
    ) -> dict:
        """
        Fetch historical OHLCV data for multiple symbols from Breeze.
        
        Args:
            symbols: List of stock symbols (e.g., ['RELIANCE', 'TCS'])
            period: Historical period (not directly used by Breeze API - handled via dates)
            interval: '1minute', '5minute', '30minute', '1day'
        
        Returns:
            Dictionary with symbol -> DataFrame mapping
        """
        data = {}
        
        # Convert period to days for Breeze API
        period_days = self._convert_period_to_days(period)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        #read pickle if present
        try:
            #read from postgres
            data = self.postgres_store.get_equity(symbols, start_date, end_date)
            # data = pickle.load(open("D:\Projects\Screener\data.pickle", "rb"))
            if data:
                #store to postgres history.equity table
                if self.postgres_store and self.postgres_store.connection:
                    for symbol, df in data.items():
                        if self.postgres_store.insert_dataframe(df, symbol):
                            logger.info(f"Successfully stored {symbol} data to PostgreSQL from pickle")
                        else:
                            logger.warning(f"Failed to store {symbol} data to PostgreSQL from pickle")
                return data
        except Exception as e:
            logger.warning(f"Could not load data: {e}")
        
        for symbol in symbols:
            try:
                # Breeze API expects instrument tokens, but we'll use symbol-based approach
                historical_data = self.breeze.get_historical_data_v2(
                    interval=interval,
                    from_date=start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    to_date=end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    exchange_code="NSE",
                    stock_code=symbol 
                    # stock_code=self._get_stock_token(symbol)
                )['Success']
                
                if historical_data and len(historical_data) > 0:
                    df = pd.DataFrame(historical_data)
                    df.columns = [str(col).lower() for col in df.columns]
                    data[symbol] = df
                    print(f"Fetched {len(df)} rows for {symbol} from Breeze")
                    
                    # Store to postgres history.equity table
                    if self.postgres_store and self.postgres_store.connection:
                        if self.postgres_store.insert_dataframe(df, symbol):
                            logger.info(f"Successfully stored {symbol} data to PostgreSQL")
                        else:
                            logger.warning(f"Failed to store {symbol} data to PostgreSQL")
                    else:
                        logger.warning(f"PostgreSQL not available. Skipping database storage for {symbol}")
                else:
                    logger.warning(f"No data returned for {symbol}")
                    
            except Exception as e:
                logger.warning(f"Failed to fetch {symbol} from Breeze: {e}")
            time.sleep(0.65)  # To respect API rate limits
        return data
    
    def get_stock_info(self, symbol: str) -> dict:
        """Get fundamental info about a stock from Breeze."""
        try:
            # Get quote data
            quote_data = self.breeze.get_quotes(
                stock_token=self._get_stock_token(symbol)
            )
            
            if quote_data:
                return {
                    'symbol': symbol,
                    'name': quote_data.get('company_name', symbol),
                    'market_cap': quote_data.get('market_cap', 0),
                    'current_price': quote_data.get('ltp', quote_data.get('close', 0)),
                    'open_price': quote_data.get('open', 0),
                    'high_price': quote_data.get('high', 0),
                    'low_price': quote_data.get('low', 0),
                    'volume': quote_data.get('volume', 0),
                    'change': quote_data.get('change', 0),
                    'change_percent': quote_data.get('change_percentage', 0),
                    '52_week_high': quote_data.get('52w_high', 0),
                    '52_week_low': quote_data.get('52w_low', 0),
                }
            return {}
        except Exception as e:
            logger.warning(f"Failed to get info for {symbol} from Breeze: {e}")
            return {}
    
    # def _get_stock_token(self, symbol: str) -> str:
    #     """
    #     Get Breeze stock token for a given symbol.
        
    #     Note: Breeze API uses instrument tokens instead of symbols directly.
    #     This is a helper method that may need symbol-to-token mapping.
    #     """
    #     try:
    #         # Attempt to get token from Breeze using symbol search
    #         search_results = self.breeze.search_scrips(symbol=symbol)
    #         if search_results and len(search_results) > 0:
    #             return search_results[0].get('token', symbol)
    #     except Exception as e:
    #         logger.warning(f"Could not get token for {symbol}: {e}")
        
    #     # Fallback to symbol itself
    #     return symbol
    
    @staticmethod
    def _convert_period_to_days(period: str) -> int:
        """Convert period string to number of days."""
        period_map = {
            '1d': 1,
            '5d': 5,
            '1mo': 30,
            '3mo': 90,
            '6mo': 180,
            '1y': 365,
            '5y': 1825,
        }
        return period_map.get(period, 90)


class YFinanceDataFetcher:
    """Fetches and caches stock data from yfinance."""
    
    def __init__(self):
        self.cache = {}
    
    def get_historical_data(
        self, 
        symbols: List[str], 
        period: str = "3mo",
        interval: str = "1d"
    ) -> dict:
        """
        Fetch historical OHLCV data for multiple symbols.
        
        Args:
            symbols: List of stock symbols (e.g., ['AAPL', 'MSFT'])
            period: '1d', '5d', '1mo', '3mo', '6mo', '1y', '5y'
            interval: '1m', '5m', '15m', '30m', '60m', '1d'
        
        Returns:
            Dictionary with symbol -> DataFrame mapping
        """
        data = {}
        for symbol in symbols:
            try:
                df = yf.download(symbol, period=period, interval=interval, progress=False)
                if not df.empty:
                    df.reset_index(inplace=True)
                    # Normalize column names while preserving MultiIndex if present
                    if isinstance(df.columns, pd.MultiIndex):
                        # Lowercase each level of the MultiIndex
                        lowered = []
                        for col in df.columns:
                            lowered.append(tuple(str(part).lower() for part in col))
                        try:
                            df.columns = pd.MultiIndex.from_tuples(lowered, names=df.columns.names)
                        except Exception:
                            # Fallback to flattened lowercase names if MultiIndex recreation fails
                            df.columns = ["_".join(map(str, col)).lower() for col in df.columns]
                    else:
                        df.columns = [str(c).lower() for c in df.columns]
                    data[symbol] = df
                    logger.info(f"Fetched {len(df)} rows for {symbol}")
            except Exception as e:
                logger.warning(f"Failed to fetch {symbol}: {e}")
        
        # Store to postgres hisotyr.equity table


        return data
    
    def get_stock_info(self, symbol: str) -> dict:
        """Get fundamental info about a stock."""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            return {
                'symbol': symbol,
                'name': info.get('longName', ''),
                'market_cap': info.get('marketCap', 0),
                'market_cap_category': self._categorize_market_cap(info.get('marketCap', 0)),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'pe_ratio': info.get('trailingPE', None),
                'dividend_yield': info.get('dividendYield', 0),
                'avg_volume': info.get('averageVolume', 0),
                'current_price': info.get('currentPrice', 0),
            }
        except Exception as e:
            logger.warning(f"Failed to get info for {symbol}: {e}")
            return {}
    
    def _categorize_market_cap(self, market_cap: float) -> str:
        """Categorize stocks by market cap."""
        if market_cap == 0:
            return "unknown"
        elif market_cap >= 300e9:
            return "mega_cap"
        elif market_cap >= 100e9:
            return "large_cap"
        elif market_cap >= 10e9:
            return "mid_cap"
        elif market_cap >= 2e9:
            return "small_cap"
        else:
            return "micro_cap"


class TechnicalAnalyzer:
    """Computes technical indicators for trend reversal and breakout detection."""
    
    @staticmethod
    def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """
        Add technical indicators to price data.
        
        Args:
            df: DataFrame with OHLCV columns (Open, High, Low, Close, Volume)
        
        Returns:
            DataFrame with additional indicator columns
        """
        df = df.copy()

        # Ensure column names are normalized but keep MultiIndex if present
        if isinstance(df.columns, pd.MultiIndex):
            # Lowercase each part of tuple column labels for matching
            df.columns = pd.MultiIndex.from_tuples(
                [tuple(str(p).lower() for p in col) for col in df.columns],
                names=df.columns.names,
            )

        else:
            df.columns = [str(col).lower() for col in df.columns]

        #convet to float for ohlcv columns
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = df[col].astype(float)

        # Map the essential OHLCV columns (some tickers return 'adj close')
        def _find(col_name):
            for c in df.columns:
                # If MultiIndex label (tuple), check any level
                if isinstance(c, tuple):
                    for part in c:
                        if col_name in str(part):
                            return c
                else:
                    if col_name in str(c):
                        return c
            return None

        open_col = _find('open')
        high_col = _find('high')
        low_col = _find('low')
        close_col = _find('close')
        volume_col = _find('volume')

        # If required columns are missing, return original frame and log warning
        if not all([open_col, high_col, low_col, close_col, volume_col]):
            logger.warning("Missing OHLCV columns for indicator calculation; skipping indicators")
            return df
        
        # Simple Moving Averages
        # Use the discovered column names for calculations
        df['sma_20'] = df[close_col].rolling(20).mean()
        df['sma_50'] = df[close_col].rolling(50).mean()
        df['sma_200'] = df[close_col].rolling(200).mean()

        # Exponential Moving Average
        df['ema_12'] = df[close_col].ewm(span=12, adjust=False).mean()
        df['ema_26'] = df[close_col].ewm(span=26, adjust=False).mean()

        # MACD
        df['macd'] = df['ema_12'] - df['ema_26']
        df['signal_line'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_histogram'] = df['macd'] - df['signal_line']

        # RSI (Relative Strength Index)
        df['rsi'] = TechnicalAnalyzer._calculate_rsi(df[close_col], period=14)

        # ATR (Average True Range)
        # Make sure ATR uses the mapped columns
        df_temp = df.rename(columns={open_col: 'open', high_col: 'high', low_col: 'low', close_col: 'close', volume_col: 'volume'})
        df['atr'] = TechnicalAnalyzer._calculate_atr(df_temp, period=14)
        df['atr_percent'] = (df['atr'] / df[close_col]) * 100

        # Bollinger Bands
        df['bb_middle'] = df[close_col].rolling(20).mean()
        bb_std = df[close_col].rolling(20).std()
        df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
        df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']

        # Volume indicators
        df['volume_sma_20'] = df[volume_col].rolling(20).mean()
        df['volume_ratio'] = df[volume_col] / df['volume_sma_20']

        # Price vs VWAP
        # For VWAP we need the proper high/low/volume mapping, use df_temp
        df['vwap'] = TechnicalAnalyzer._calculate_vwap(df_temp)
        df['price_vs_vwap'] = ((df[close_col] - df['vwap']) / df['vwap']) * 100
        
        return df
    
    @staticmethod
    def _calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index."""
        #convert to float(2) before calculations
        prices = prices.astype(float)
        deltas = prices.diff()
        seed = deltas[:period+1]
        up = seed[seed >= 0].sum() / period
        down = -seed[seed < 0].sum() / period
        rs = up / down
        rsi = np.zeros_like(prices)
        rsi[:period] = 100. - 100. / (1. + rs)
        
        for i in range(period, len(prices)):
            delta = deltas.iloc[i]
            if delta > 0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = -delta
            
            up = (up * (period - 1) + upval) / period
            down = (down * (period - 1) + downval) / period
            rs = up / down
            rsi[i] = 100. - 100. / (1. + rs)
        
        return pd.Series(rsi, index=prices.index)
    
    @staticmethod
    def _calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range."""
        # Select series safely (handles MultiIndex or multiple matching columns)
        high = TechnicalAnalyzer._select_series(df, 'high')
        low = TechnicalAnalyzer._select_series(df, 'low')
        close = TechnicalAnalyzer._select_series(df, 'close')

        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()

        return atr
    
    @staticmethod
    def _calculate_vwap(df: pd.DataFrame) -> pd.Series:
        """Calculate Volume Weighted Average Price."""
        df = df.copy()
        # Safely select high/low/volume series
        high = TechnicalAnalyzer._select_series(df, 'high')
        low = TechnicalAnalyzer._select_series(df, 'low')
        volume = TechnicalAnalyzer._select_series(df, 'volume')

        hl_avg = (high + low) / 2
        cum_vol = volume.cumsum()
        cum_hlv = (hl_avg * volume).cumsum()
        vwap = cum_hlv / cum_vol.replace(0, np.nan)
        vwap.name = 'vwap'
        return vwap

    @staticmethod
    def _select_series(df: pd.DataFrame, name: str) -> pd.Series:
        """Return a single Series for `name`, handling MultiIndex or multiple matches.

        Preference order:
        - Exact column name match
        - First matching level in MultiIndex tuple
        - First column whose string contains `name`
        """
        # Exact match
        if name in df.columns:
            series = df[name]
            if isinstance(series, pd.DataFrame):
                return series.iloc[:, 0]
            return series

        # MultiIndex: search tuple parts
        if isinstance(df.columns, pd.MultiIndex):
            for col in df.columns:
                for part in col:
                    if name == str(part).lower():
                        series = df[col]
                        if isinstance(series, pd.DataFrame):
                            return series.iloc[:, 0]
                        return series

        # Fallback: substring match in column names
        for col in df.columns:
            if name in str(col):
                series = df[col]
                if isinstance(series, pd.DataFrame):
                    return series.iloc[:, 0]
                return series

        # As last resort, raise KeyError
        raise KeyError(f"No column matching '{name}' found in DataFrame")


class ScreenerDataProvider:
    """Orchestrates data fetching and preparation for screening."""
    
    def __init__(self, data_source: Optional[str] = None):
        """
        Initialize ScreenerDataProvider with specified data source.
        
        Args:
            data_source: 'breeze', 'yfinance', or None (auto-detect from env or defaults to yfinance)
        """

        data_source = os.getenv('DATA_SOURCE', 'yfinance')

        print(f"Initializing Data Provider with data_source: {data_source}")
        
        try:
            if data_source.lower() == 'breeze':
                self.fetcher = BreezeFetcher()
                logger.info("Using ICICI Breeze API as data source")

            elif data_source.lower() == 'yfinance':
                self.fetcher = YFinanceDataFetcher()
                logger.info("Using yfinance as data source")

        except Exception as e:
            logger.error(f"Failed to initialize Breeze: {e}. Falling back to yfinance.")
            self.fetcher = YFinanceDataFetcher()
            self.data_source = 'yfinance'
        
        self.analyzer = TechnicalAnalyzer()
    