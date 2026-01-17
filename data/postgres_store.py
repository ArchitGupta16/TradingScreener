"""
PostgreSQL database operations for storing raw equity/market data.
Handles connections and data insertion to the history database.
"""

import logging
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class PostgresStore:
    """Handle PostgreSQL database operations for raw equity market data."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """
        Initialize PostgreSQL connection parameters.
        
        Args:
            host: Database host address
            port: Database port
            database: Database name (e.g., 'history')
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
    
    def connect(self) -> bool:
        """
        Establish connection to PostgreSQL database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"Connected to PostgreSQL database: {self.database}")
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from PostgreSQL database")
    
    def create_equity_table(self) -> bool:
        """
        Create equity table for raw OHLCV data if it doesn't exist.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            logger.error("Not connected to database")
            return False
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS equity (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            date TIMESTAMP NOT NULL,
            open DECIMAL(12, 4),
            high DECIMAL(12, 4),
            low DECIMAL(12, 4),
            close DECIMAL(12, 4),
            volume BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, date)
        );
        CREATE INDEX IF NOT EXISTS idx_equity_symbol ON equity(symbol);
        CREATE INDEX IF NOT EXISTS idx_equity_date ON equity(date);
        CREATE INDEX IF NOT EXISTS idx_equity_symbol_date ON equity(symbol, date);
        """
        
        try:
            cursor = self.connection.cursor()
            # Execute each statement separately
            for statement in create_table_sql.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            self.connection.commit()
            logger.info("Equity table created or verified")
            cursor.close()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to create equity table: {e}")
            self.connection.rollback()
            return False
    
    def insert_equity(self, symbol: str, date: datetime, 
                     open_price: float, high: float, low: float, 
                     close: float, volume: int) -> bool:
        """
        Insert a single raw OHLCV equity record.
        
        Args:
            symbol: Stock ticker symbol
            date: Date of the OHLCV data
            open_price: Opening price
            high: High price
            low: Low price
            close: Closing price
            volume: Trading volume
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            logger.error("Not connected to database")
            return False
        
        insert_sql = """
        INSERT INTO equity (symbol, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(insert_sql, (symbol, date, open_price, high, low, close, volume))
            self.connection.commit()
            logger.info(f"Inserted/updated equity record for {symbol} on {date}")
            cursor.close()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to insert equity record: {e}")
            self.connection.rollback()
            return False
    
    def insert_equity_batch(self, records: List[Dict[str, Any]]) -> bool:
        """
        Insert multiple raw OHLCV equity records in batch.
        
        Args:
            records: List of dictionaries containing equity data
                    Each dict should have keys: symbol, date, open, high, low, close, volume
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            logger.error("Not connected to database")
            return False
        
        if not records:
            logger.warning("No records to insert")
            return False
        
        insert_sql = """
        INSERT INTO equity (symbol, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
        """
        
        try:
            cursor = self.connection.cursor()
            data = [
                (
                    record.get('symbol'),
                    record.get('date'),
                    record.get('open'),
                    record.get('high'),
                    record.get('low'),
                    record.get('close'),
                    record.get('volume')
                )
                for record in records
            ]
            execute_batch(cursor, insert_sql, data, page_size=100)
            self.connection.commit()
            logger.info(f"Inserted {len(records)} equity records")
            cursor.close()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to insert equity records: {e}")
            self.connection.rollback()
            return False
    
    def insert_dataframe(self, df: pd.DataFrame, symbol: str) -> bool:
        """
        Insert OHLCV data from a pandas DataFrame.
        Useful for direct integration with yfinance/Breeze data.
        
        Args:
            df: DataFrame with columns: open, high, low, close, volume
                Index should be datetime
            symbol: Stock symbol
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection or df.empty:
            logger.error("Not connected to database or DataFrame is empty")
            return False
        
        records = []
        for date, row in df.iterrows():
            # Handle both DatetimeIndex and regular index
            if hasattr(date, 'to_pydatetime'):
                date_val = date.to_pydatetime()
            else:
                date_val = pd.to_datetime(date)
            
            records.append({
                'symbol': symbol,
                'date': date_val,
                'open': float(row.get('Open', row.get('open', 0))),
                'high': float(row.get('High', row.get('high', 0))),
                'low': float(row.get('Low', row.get('low', 0))),
                'close': float(row.get('Close', row.get('close', 0))),
                'volume': int(row.get('Volume', row.get('volume', 0)))
            })
        
        return self.insert_equity_batch(records)
    
    def update_equity(self, symbol: str, date: datetime, **kwargs) -> bool:
        """
        Update an equity record.
        
        Args:
            symbol: Stock ticker symbol
            date: Date of the record
            **kwargs: Fields to update (e.g., close=100.50, volume=1000000)
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection or not kwargs:
            logger.error("Not connected to database or no fields to update")
            return False
        
        allowed_fields = {'open', 'high', 'low', 'close', 'volume'}
        update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}
        
        if not update_fields:
            logger.warning("No valid fields to update")
            return False
        
        set_clause = ", ".join([f"{k} = %s" for k in update_fields.keys()])
        update_sql = f"UPDATE equity SET {set_clause} WHERE symbol = %s AND date = %s"
        
        try:
            cursor = self.connection.cursor()
            values = list(update_fields.values()) + [symbol, date]
            cursor.execute(update_sql, values)
            self.connection.commit()
            logger.info(f"Updated equity record for {symbol} on {date}")
            cursor.close()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to update equity record: {e}")
            self.connection.rollback()
            return False
    
    def get_equity(self, symbol: str, date: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieve an equity record by symbol and optionally by date.
        
        Args:
            symbol: Stock ticker symbol
            date: Optional specific date to retrieve
        
        Returns:
            Dictionary containing the record or None if not found
        """
        if not self.connection:
            logger.error("Not connected to database")
            return None
        
        if date:
            select_sql = "SELECT * FROM equity WHERE symbol = %s AND date = %s"
            params = (symbol, date)
        else:
            select_sql = "SELECT * FROM equity WHERE symbol = %s ORDER BY date DESC LIMIT 1"
            params = (symbol,)
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(select_sql, params)
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return {
                    'id': result[0],
                    'symbol': result[1],
                    'date': result[2],
                    'open': float(result[3]) if result[3] else None,
                    'high': float(result[4]) if result[4] else None,
                    'low': float(result[5]) if result[5] else None,
                    'close': float(result[6]) if result[6] else None,
                    'volume': result[7],
                    'created_at': result[8]
                }
            return None
        except psycopg2.Error as e:
            logger.error(f"Failed to retrieve equity record: {e}")
            return None
    
    def get_all_equities(self, symbol: Optional[str] = None, 
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Retrieve equity records with optional filtering.
        
        Args:
            symbol: Optional stock symbol to filter by
            start_date: Optional start date
            end_date: Optional end date
        
        Returns:
            List of dictionaries containing equity records
        """
        if not self.connection:
            logger.error("Not connected to database")
            return []
        
        select_sql = "SELECT * FROM equity WHERE 1=1"
        params = []
        
        if symbol:
            select_sql += " AND symbol = %s"
            params.append(symbol)
        if start_date:
            select_sql += " AND date >= %s"
            params.append(start_date)
        if end_date:
            select_sql += " AND date <= %s"
            params.append(end_date)
        
        select_sql += " ORDER BY symbol, date"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(select_sql, params)
            results = cursor.fetchall()
            cursor.close()
            
            equities = []
            for result in results:
                equities.append({
                    'id': result[0],
                    'symbol': result[1],
                    'date': result[2],
                    'open': float(result[3]) if result[3] else None,
                    'high': float(result[4]) if result[4] else None,
                    'low': float(result[5]) if result[5] else None,
                    'close': float(result[6]) if result[6] else None,
                    'volume': result[7],
                    'created_at': result[8]
                })
            return equities
        except psycopg2.Error as e:
            logger.error(f"Failed to retrieve equity records: {e}")
            return []
    
    def delete_equity(self, symbol: str) -> bool:
        """
        Delete an equity record.
        
        Args:
            symbol: Stock ticker symbol
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            logger.error("Not connected to database")
            return False
        
        delete_sql = "DELETE FROM equity WHERE symbol = %s"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(delete_sql, (symbol,))
            self.connection.commit()
            logger.info(f"Deleted equity record for {symbol}")
            cursor.close()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to delete equity record: {e}")
            self.connection.rollback()
            return False
