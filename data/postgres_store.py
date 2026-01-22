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
    
    def create_equity_daily_table(self) -> bool:
        """
        Create equity_daily table for raw OHLCV data if it doesn't exist.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            logger.error("Not connected to database")
            return False
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS equity_daily (
            id SERIAL,
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
        CREATE INDEX IF NOT EXISTS idx_equity_symbol ON equity_daily(symbol);
        CREATE INDEX IF NOT EXISTS idx_equity_date ON equity_daily(date);
        CREATE INDEX IF NOT EXISTS idx_equity_symbol_date ON equity_daily(symbol, date);
        """
        
        try:
            cursor = self.connection.cursor()
            # Execute each statement separately
            for statement in create_table_sql.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            self.connection.commit()
            logger.info("equity_daily table created or verified")
            cursor.close()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to create equity_daily table: {e}")
            self.connection.rollback()
            return False
    
    def insert_data(self, df: pd.DataFrame, query) -> bool:
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
            records.append({
                'symbol': row.get('symbol'),
                'date': row.get('datetime', 'date'),
                'open': float(row.get('Open', row.get('open', 0))),
                'high': float(row.get('High', row.get('high', 0))),
                'low': float(row.get('Low', row.get('low', 0))),
                'close': float(row.get('Close', row.get('close', 0))),
                'volume': int(row.get('Volume', row.get('volume', 0)))
            })
        
        # Use upsert (ON CONFLICT) for updating existing records
        # insert_sql = """
        # INSERT INTO equity_daily (symbol, date, open, high, low, close, volume)
        # VALUES (%s, %s, %s, %s, %s, %s, %s)
        # ON CONFLICT (symbol, date) DO UPDATE SET
        # open = EXCLUDED.open,
        # high = EXCLUDED.high,
        # low = EXCLUDED.low,
        # close = EXCLUDED.close,
        # volume = EXCLUDED.volume
        # """

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
            execute_batch(cursor, query, data, page_size=100)
            self.connection.commit()
            logger.info(f"Upserted {len(records)} equity_daily records")
            cursor.close()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to upsert equity_daily records: {e}")
            self.connection.rollback()
            return False

    def get_equity_daily(self, symbol: str, start_date, end_date, query) -> Optional[Dict[str, Any]]:
        """
        Retrieve an equity_daily record by symbol and optionally by date.
        
        Args:
            symbol: Stock ticker symbol
            date: Optional specific date to retrieve
        
        Returns:
            Dictionary containing the record or None if not found
        """
        if not self.connection:
            logger.error("Not connected to database")
            return None
        
        start_date = start_date.strftime("%Y-%m-%d") if start_date else None
        end_date = end_date.strftime("%Y-%m-%d") if end_date else None
        symbols = ", ".join(f"'{s}'" for s in symbol) if symbol else []

        if start_date and end_date:
            select_sql = f"SELECT * FROM equity_daily WHERE symbol in ({symbols}) AND date BETWEEN '{start_date}' AND '{end_date}'"
        else:
            select_sql = f"SELECT * FROM equity_daily WHERE symbol in ({symbols}) ORDER BY date DESC LIMIT 1"
        
        if len(symbols) == 0:
            select_sql = query
            
        try:
            cursor = self.connection.cursor()
            cursor.execute(select_sql)
            result = cursor.fetchall()
            cursor.close()

            if result:
                return pd.DataFrame(result,columns=[desc[0] for desc in cursor.description])
            else:
                print("No data found")
                return None
        except psycopg2.Error as e:
            logger.error(f"Failed to retrieve equity_daily record: {e}")
            return None
    