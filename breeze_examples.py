#!/usr/bin/env python3
"""
Quick Examples: Using Breeze API Integration

Run these examples to test your Breeze integration.
Make sure you have set up .env with valid credentials first!
"""

# Example 1: Basic ScreenerDataProvider with Breeze
def example_basic_usage():
    """Most common usage pattern."""
    from data.fetcher import ScreenerDataProvider
    
    # This will automatically use Breeze if DATA_SOURCE=breeze in .env
    provider = ScreenerDataProvider()
    print(f"Using data source: {provider.data_source}")


# Example 2: Direct BreezeFetcher Usage
def example_direct_breeze():
    """Using BreezeFetcher directly for more control."""
    from data.fetcher import BreezeFetcher
    
    try:
        fetcher = BreezeFetcher()
        
        # Get historical data for multiple stocks
        symbols = ['RELIANCE', 'TCS', 'INFY']
        data = fetcher.get_historical_data(
            symbols=symbols,
            period='3mo',
            interval='1d'
        )
        
        print(f"Fetched data for {len(data)} symbols")
        
        # Get stock information
        for symbol in symbols:
            info = fetcher.get_stock_info(symbol)
            if info:
                print(f"{symbol}: ₹{info.get('current_price', 'N/A')}")
                
    except Exception as e:
        print(f"Error: {e}")


# Example 3: Explicit Data Source Selection
def example_select_source():
    """Choose data source explicitly."""
    from data.fetcher import ScreenerDataProvider
    
    # Force using Breeze
    provider_breeze = ScreenerDataProvider(data_source='breeze')
    print(f"Breeze provider: {provider_breeze.data_source}")
    
    # Force using yfinance
    provider_yf = ScreenerDataProvider(data_source='yfinance')
    print(f"YFinance provider: {provider_yf.data_source}")


# Example 4: Handling Multiple Stocks
def example_batch_processing():
    """Process multiple stocks."""
    from data.fetcher import BreezeFetcher
    
    try:
        fetcher = BreezeFetcher()
        
        # List of Indian stocks
        stocks = ['RELIANCE', 'TCS', 'INFY', 'WIPRO', 'LT', 'SBIN']
        
        # Fetch all at once
        historical = fetcher.get_historical_data(stocks, period='1mo')
        
        # Process results
        for symbol, df in historical.items():
            if not df.empty:
                latest_price = df.iloc[-1]['close'] if 'close' in df.columns else 'N/A'
                print(f"{symbol}: Latest price = {latest_price}")
            else:
                print(f"{symbol}: No data available")
                
    except Exception as e:
        print(f"Error: {e}")


# Example 5: Integration with Technical Analysis
def example_with_indicators():
    """Get data and add technical indicators."""
    from data.fetcher import ScreenerDataProvider
    
    try:
        provider = ScreenerDataProvider(data_source='breeze')
        
        # Get historical data
        data = provider.fetcher.get_historical_data(['RELIANCE'], period='6mo')
        
        # Add technical indicators
        for symbol, df in data.items():
            df_with_indicators = provider.analyzer.add_indicators(df)
            
            # Get latest analysis
            latest = df_with_indicators.iloc[-1]
            print(f"{symbol} Latest Analysis:")
            print(f"  RSI: {latest.get('rsi', 'N/A')}")
            print(f"  MACD: {latest.get('macd', 'N/A')}")
            print(f"  SMA 200: {latest.get('sma_200', 'N/A')}")
            
    except Exception as e:
        print(f"Error: {e}")


# Example 6: Error Handling with Fallback
def example_error_handling():
    """Demonstrates fallback behavior."""
    from data.fetcher import ScreenerDataProvider
    
    # This will try Breeze first, fallback to yfinance if needed
    provider = ScreenerDataProvider(data_source='breeze')
    
    print(f"Using: {provider.data_source}")
    print("If Breeze failed, automatically using yfinance instead")
    
    # Your code continues normally
    try:
        data = provider.fetcher.get_historical_data(['RELIANCE'])
        print(f"Successfully fetched data using {provider.data_source}")
    except Exception as e:
        print(f"Error: {e}")


# Example 7: Custom Credentials (alternative to .env)
def example_custom_credentials():
    """Using credentials passed directly (not from .env)."""
    from data.fetcher import BreezeFetcher
    
    try:
        # Pass credentials directly (alternative to .env)
        fetcher = BreezeFetcher(
            api_key='your_api_key_here',
            api_secret='your_api_secret_here'
        )
        
        data = fetcher.get_historical_data(['RELIANCE'])
        print(f"Fetched data with custom credentials")
        
    except Exception as e:
        print(f"Error: {e}")


# Example 8: Checking Available Data Sources
def example_check_source():
    """Check which data source is being used."""
    from data.fetcher import ScreenerDataProvider
    import os
    
    # Check environment variable
    env_source = os.getenv('DATA_SOURCE', 'not set')
    print(f"DATA_SOURCE environment variable: {env_source}")
    
    # Check what provider uses
    provider = ScreenerDataProvider()
    print(f"Provider using: {provider.data_source}")


# Run a specific example
if __name__ == '__main__':
    import sys
    
    examples = {
        '1': ('Basic Usage', example_basic_usage),
        '2': ('Direct Breeze Fetcher', example_direct_breeze),
        '3': ('Select Data Source', example_select_source),
        '4': ('Batch Processing', example_batch_processing),
        '5': ('With Indicators', example_with_indicators),
        '6': ('Error Handling', example_error_handling),
        '7': ('Custom Credentials', example_custom_credentials),
        '8': ('Check Data Source', example_check_source),
    }
    
    print("Breeze Integration Examples")
    print("=" * 50)
    
    for key, (name, _) in examples.items():
        print(f"{key}. {name}")
    
    print("0. Exit")
    
    choice = input("\nChoose an example (0-8): ").strip()
    
    if choice in examples:
        print(f"\nRunning: {examples[choice][0]}")
        print("-" * 50)
        try:
            examples[choice][1]()
        except Exception as e:
            print(f"Error running example: {e}")
    elif choice != '0':
        print("Invalid choice")
