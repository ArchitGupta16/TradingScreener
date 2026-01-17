"""
Integration module for storing fetched data to PostgreSQL.
Bridges the fetcher module with the PostgreSQL store.
"""

import logging
from typing import List, Dict, Optional
from postgres_store import PostgresStore
from fetcher import BreezeFetcher, StockDataFetcher
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


class DataStorageManager:
    """Manages fetching and storing raw equity data to PostgreSQL."""
    
    def __init__(self, 
                 db_host: str = None, 
                 db_port: int = None,
                 db_name: str = None,
                 db_user: str = None,
                 db_password: str = None,
                 use_breeze: bool = False):
        """
        Initialize the storage manager.
        
        Args:
            db_host: PostgreSQL host (defaults to DB_HOST env var)
            db_port: PostgreSQL port (defaults to DB_PORT env var)
            db_name: Database name (defaults to DB_NAME env var, e.g., 'history')
            db_user: Database user (defaults to DB_USER env var)
            db_password: Database password (defaults to DB_PASSWORD env var)
            use_breeze: Use Breeze API instead of yfinance
        """
        self.db_host = db_host or os.getenv('DB_HOST', 'localhost')
        self.db_port = int(db_port or os.getenv('DB_PORT', 5432))
        self.db_name = db_name or os.getenv('DB_NAME', 'history')
        self.db_user = db_user or os.getenv('DB_USER', 'postgres')
        self.db_password = db_password or os.getenv('DB_PASSWORD', '')
        
        self.store = PostgresStore(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            password=self.db_password
        )
        
        if use_breeze:
            self.fetcher = BreezeFetcher()
        else:
            self.fetcher = StockDataFetcher()
    
    def connect_database(self) -> bool:
        """Connect to PostgreSQL database."""
        if not self.store.connect():
            logger.error("Failed to connect to database")
            return False
        
        if not self.store.create_equity_table():
            logger.error("Failed to create/verify equity table")
            return False
        
        logger.info("Database connection and table setup successful")
        return True
    
    def fetch_and_store(self, 
                       symbols: List[str], 
                       period: str = "3mo",
                       interval: str = "1d") -> bool:
        """
        Fetch historical data and store it to PostgreSQL.
        
        Args:
            symbols: List of stock symbols
            period: Historical period ('1d', '5d', '1mo', '3mo', '6mo', '1y', '5y')
            interval: Data interval ('1m', '5m', '15m', '30m', '60m', '1d')
        
        Returns:
            bool: True if successful
        """
        try:
            logger.info(f"Fetching data for {len(symbols)} symbols...")
            historical_data = self.fetcher.get_historical_data(symbols, period=period, interval=interval)
            
            if not historical_data:
                logger.warning("No data fetched")
                return False
            
            stored_count = 0
            for symbol, df in historical_data.items():
                if df.empty:
                    logger.warning(f"Empty dataframe for {symbol}")
                    continue
                
                if self.store.insert_dataframe(df, symbol):
                    stored_count += 1
                    logger.info(f"Stored {len(df)} records for {symbol}")
                else:
                    logger.error(f"Failed to store data for {symbol}")
            
            logger.info(f"Successfully stored data for {stored_count}/{len(symbols)} symbols")
            return stored_count > 0
        
        except Exception as e:
            logger.error(f"Error in fetch_and_store: {e}")
            return False
    
    def fetch_and_store_single(self, symbol: str, period: str = "3mo") -> bool:
        """
        Fetch and store data for a single symbol.
        
        Args:
            symbol: Stock symbol
            period: Historical period
        
        Returns:
            bool: True if successful
        """
        return self.fetch_and_store([symbol], period=period)
    
    def get_stored_data(self, 
                       symbol: str, 
                       start_date=None, 
                       end_date=None) -> Dict:
        """
        Retrieve stored data from PostgreSQL.
        
        Args:
            symbol: Stock symbol
            start_date: Optional start date filter
            end_date: Optional end date filter
        
        Returns:
            Dictionary with raw OHLCV data
        """
        records = self.store.get_all_equities(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            'symbol': symbol,
            'data': records,
            'count': len(records)
        }
    
    def close(self):
        """Close database connection."""
        self.store.disconnect()


# Example usage
if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO)
    
    # Initialize manager
    manager = DataStorageManager(use_breeze=False)
    
    if not manager.connect_database():
        exit(1)
    
    symbols = ['RELIANCE', 'TCS', 'INFY']
    if manager.fetch_and_store(symbols, period='3mo'):
        print("Data stored successfully!")
        
        result = manager.get_stored_data('RELIANCE')
        print(f"\nStored {result['count']} records for {result['symbol']}")
        if result['data']:
            first = result['data'][0]
            print(f"Sample: {first}")
    
    manager.close()
