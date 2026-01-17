"""
Technical pattern detection for trend reversal and breakout scenarios.
"""
import pandas as pd
import numpy as np


class PatternDetector:
    """Detects technical patterns indicating trend reversals and breakouts."""
    
    @staticmethod
    def _as_scalar(value, default=None):
        """Coerce pandas Series/ndarray or numpy types to Python scalar if possible."""
        if value is None:
            return default
        if isinstance(value, (pd.Series,)):
            if value.empty:
                return default
            try:
                return float(value.iloc[-1])
            except Exception:
                return default
        if isinstance(value, (np.ndarray,)):
            if value.size == 0:
                return default
            try:
                return float(value.flatten()[-1])
            except Exception:
                return default
        # numpy scalar
        try:
            if hasattr(value, 'item'):
                return value.item()
        except Exception:
            pass
        return value

    @staticmethod
    def detect_trend_reversal(df: pd.DataFrame, lookback: int = 20) -> dict:
        """
        Detect potential trend reversal signals.
        
        Signals include:
        - Oversold RSI (< 30) with bullish divergence
        - Price testing lower Bollinger Band
        - MACD bullish crossover
        
        Returns:
            Dictionary with reversal_score (0-100) and signals list
        """
        if len(df) < lookback:
            return {'reversal_score': 0, 'signals': []}
        
        latest = df.iloc[-1]
        signals = []
        score = 0
        
        # Helper to get scalar from latest row
        def get(key, default=None):
            return PatternDetector._as_scalar(latest.get(key, default), default)

        # RSI Oversold Check
        if get('rsi', 50) < 30:
            signals.append("RSI Oversold (<30)")
            score += 20

        # Price testing lower Bollinger Band
        if get('close', 0) <= get('bb_lower', get('close', 0)):
            signals.append("Price at lower Bollinger Band")
            score += 15

        # MACD Bullish Crossover
        if len(df) >= 2:
            prev_macd_hist = PatternDetector._as_scalar(df.iloc[-2].get('macd_histogram', 0), 0)
            curr_macd_hist = get('macd_histogram', 0)

            if prev_macd_hist < 0 and curr_macd_hist > 0:
                signals.append("MACD Bullish Crossover")
                score += 25

        # Recent support bounce
        recent_low = PatternDetector._as_scalar(df['close'].tail(lookback).min(), None)
        curr_close = get('close', None)
        if recent_low is not None and curr_close is not None:
            if curr_close > recent_low and (curr_close - recent_low) / recent_low > 0.02:
                if curr_close / recent_low < 1.05:  # Near support
                    signals.append("Bounce from recent support")
                    score += 15

        # High volatility (ATR expanding)
        if get('atr_percent', 0) > 2.0:
            signals.append("High volatility (ATR > 2%)")
            score += 10

        # Volume confirmation
        if get('volume_ratio', 1) > 1.2:
            signals.append("Above-average volume")
            score += 15
        
        return {
            'reversal_score': min(score, 100),
            'signals': signals,
            'rsi': latest.get('rsi', None),
            'atr_percent': latest.get('atr_percent', None)
        }
    
    @staticmethod
    def detect_breakout(df: pd.DataFrame, lookback: int = 20) -> dict:
        """
        Detect potential breakout scenarios (consolidation breaking).
        
        Signals include:
        - Price breakout above 52-week high
        - Breaking above upper Bollinger Band with volume
        - Price above all major moving averages (uptrend)
        
        Returns:
            Dictionary with breakout_score (0-100) and signals list
        """
        if len(df) < lookback:
            return {'breakout_score': 0, 'signals': []}
        
        latest = df.iloc[-1]
        signals = []
        score = 0
        
        # Helper to get scalar from latest row
        def get(key, default=None):
            return PatternDetector._as_scalar(latest.get(key, default), default)

        # Price above all moving averages
        if (get('close', 0) > get('sma_20', 0) and
            get('sma_20', 0) > get('sma_50', 0)):
            signals.append("Price above SMA 20 > SMA 50 (uptrend)")
            score += 20

        # Breaking upper Bollinger Band
        if get('close', 0) > get('bb_upper', get('close', 0)):
            signals.append("Price broke above upper Bollinger Band")
            score += 20

        # Volume surge on breakout
        if get('volume_ratio', 1) > 1.5:
            signals.append("Strong volume breakout (>1.5x avg)")
            score += 25

        # 52-week high
        high_52w = PatternDetector._as_scalar(df['close'].tail(252).max(), None)  # Approximate 1 year
        curr_close = get('close', None)
        if high_52w is not None and curr_close is not None and len(df) >= 252 and curr_close > high_52w * 0.95:
            signals.append("Near or above 52-week high")
            score += 20

        # MACD positive and rising
        if get('macd', 0) > 0 and get('macd', 0) > get('signal_line', 0):
            signals.append("MACD positive and above signal line")
            score += 15

        # RSI not overbought (< 70)
        if get('rsi', 50) < 70:
            signals.append("Room to run (RSI < 70)")
            score += 10
        
        return {
            'breakout_score': min(score, 100),
            'signals': signals,
            'volume_ratio': latest.get('volume_ratio', None),
            'rsi': latest.get('rsi', None)
        }
    
    @staticmethod
    def calculate_composite_score(df: pd.DataFrame) -> dict:
        """
        Calculate combined reversal + breakout score for each stock.
        
        Returns:
            Dictionary with individual scores and composite recommendation
        """
        reversal = PatternDetector.detect_trend_reversal(df)
        breakout = PatternDetector.detect_breakout(df)
        
        # Weighted composite
        composite_score = (reversal['reversal_score'] * 0.4 + 
                          breakout['breakout_score'] * 0.6)
        
        return {
            'reversal_score': reversal['reversal_score'],
            'reversal_signals': reversal['signals'],
            'breakout_score': breakout['breakout_score'],
            'breakout_signals': breakout['signals'],
            'composite_score': composite_score,
            'recommendation': PatternDetector._get_recommendation(composite_score)
        }
    
    @staticmethod
    def _get_recommendation(score: float) -> str:
        """Get actionable recommendation based on score."""
        if score >= 75:
            return "Strong Setup"
        elif score >= 60:
            return "Good Setup"
        elif score >= 45:
            return "Monitor"
        else:
            return "Weak Setup"


def summarize_features(row: dict) -> str:
    return f"""
Volatility: {row.get('volatility_rank')}
Volume: {row.get('volume_ratio')}
Trend: {row.get('trend')}
Liquidity: {row.get('liquidity')}
"""
