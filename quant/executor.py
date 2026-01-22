import omegaconf
import pandas as pd
from schemas.screen_logic import QuantContract
from quant.features import PatternDetector
from data.fetcher import ScreenerDataProvider, TechnicalAnalyzer


def execute_quant_contract(
    data: pd.DataFrame,
    contract: QuantContract
) -> pd.DataFrame:
    """Execute screening contract with filters and ranking."""
    df = data.copy()

    # Apply filters
    for f in contract.filters:
        if f.metric not in df.columns:
            continue

        if f.condition == "greater_than":
            threshold = df[f.metric].median()
            df = df[df[f.metric] > threshold]
        
        elif f.condition == "less_than":
            threshold = df[f.metric].median()
            df = df[df[f.metric] < threshold]
        
        elif f.condition == "equals":
            df = df[df[f.metric] == f.value]

    return df


def screen_for_patterns(
    symbols: list,
    pattern_type: str = "both",
    queries: omegaconf = None,
    min_score: int = 50
) -> pd.DataFrame:
    """
    Screen stocks for trend reversal and/or breakout patterns.
    
    Args:
        data_provider: ScreenerDataProvider instance
        symbols: List of stock symbols to screen
        pattern_type: 'reversal', 'breakout', or 'both'
        min_score: Minimum composite score (0-100)
    
    Returns:
        DataFrame with matched stocks and pattern scores
    """
    
    data_provider = ScreenerDataProvider()
    # Fetch historical data
    historical = data_provider.fetcher.get_historical_data(symbols, queries, period="6mo")
    
    results = []
    all_stock_df = []
    for symbol in historical['symbol'].unique():
        
        df = historical[historical['symbol'] == symbol].copy()
        if df.empty or len(df) < 20:
            continue
        
        try:
            # Add technical indicators
            df_with_indicators = data_provider.analyzer.add_indicators(df)
            
            # Calculate pattern scores
            scores = PatternDetector.calculate_composite_score(df_with_indicators)
            
            # Get stock info
            # info = data_provider.fetcher.get_stock_info(symbol)
            
            # Filter by pattern type and score
            include = False
            if pattern_type == "reversal" and scores['reversal_score'] >= min_score:
                include = True
            elif pattern_type == "breakout" and scores['breakout_score'] >= min_score:
                include = True
            elif pattern_type == "both" and scores['composite_score'] >= min_score:
                include = True
            
            latest = df_with_indicators.iloc[-1]
            result = {
                'symbol': symbol,
                # 'name': info.get('name', ''),
                # 'market_cap_category': info.get('market_cap_category', ''),
                # 'current_price': info.get('current_price', PatternDetector._as_scalar(latest.get('close', 0))),
                # 'market_cap': info.get('market_cap', 0),
                'reversal_score': scores['reversal_score'],
                'breakout_score': scores['breakout_score'],
                'composite_score': scores['composite_score'],
                'recommendation': scores['recommendation'],
                'rsi': PatternDetector._as_scalar(latest.get('rsi', None)),
                'atr_percent': PatternDetector._as_scalar(latest.get('atr_percent', None)),
                'volume_ratio': PatternDetector._as_scalar(latest.get('volume_ratio', None)),
            }
            if include:
                results.append(result)
            else:
                all_stock_df.append(result)

        except Exception as e:
            print(f"Error processing {symbol}: {e}")
    
    result_df = pd.DataFrame(results)
    
    if not result_df.empty:
        # Sort by composite score
        result_df = result_df.sort_values('composite_score', ascending=False)
    
    return result_df, pd.DataFrame(all_stock_df)

