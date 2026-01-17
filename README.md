# Stock Screener with AI-Driven Trend Reversal & Breakout Detection

A sophisticated stock screener that combines **Natural Language Processing** with **Technical Analysis** to identify stocks with high probability of trend reversals and breakouts.

## Features

### 🧠 AI Intent Interpretation
- Understands natural language queries from users
- Extracts investment style (INTRADAY, SWING, LONG_TERM)
- Identifies target market and screening criteria

### 📊 Technical Pattern Detection

#### Trend Reversal Detection
Identifies stocks likely to reverse downtrends:
- **Oversold RSI** (< 30) - Classic reversal signal
- **Lower Bollinger Band Touch** - Price at support
- **MACD Bullish Crossover** - Momentum turning positive
- **Support Bounce** - Price bouncing from recent lows
- **Volume Confirmation** - Above-average volume on reversal
- **Volatility Expansion** - ATR > 2% suggesting imminent move

#### Breakout Detection
Identifies stocks likely to break out from consolidation:
- **Uptrend Formation** - Price > SMA20 > SMA50
- **Upper Bollinger Band Break** - Breaking above resistance
- **Volume Surge** - Strong volume supporting breakout
- **52-Week High** - Breaking previous highs
- **MACD Bullish** - Momentum in positive territory
- **RSI Room to Run** - RSI < 70 (not overbought yet)

### 📈 Technical Indicators
- **Moving Averages**: SMA (20, 50, 200), EMA (12, 26)
- **Momentum**: RSI (14), MACD, Signal Line
- **Volatility**: ATR, Bollinger Bands, Band Width
- **Volume**: Volume Ratio, VWAP, Volume-weighted analysis

### 🎯 Scoring System
- **Reversal Score** (0-100): Probability of trend reversal
- **Breakout Score** (0-100): Probability of breakout
- **Composite Score**: Weighted average (40% reversal, 60% breakout)
- **Recommendations**: Strong Setup / Good Setup / Monitor / Weak Setup

## Installation

### Prerequisites
- Python 3.10+
- Conda or pip package manager

### Setup

```bash
cd d:\Projects\Screener

# Install dependencies
pip install -r requirements.txt

# Additional packages (if not in requirements.txt)
pip install yfinance ta rich
```

## Usage

### Interactive Mode (Recommended)

```bash
python main.py
```

Then enter natural language requests like:
```
✓ "Give me 5 midcap stocks with high chances of trend reversal"
✓ "Find 10 largecap stocks set for breakout"
✓ "Show me small cap stocks with reversal patterns"
✓ "Screen intraday trading opportunities with strong volume"
```

The screener will:
1. Parse your intent with LLM
2. Generate screening criteria
3. Fetch live market data from yfinance
4. Analyze technical patterns
5. Return ranked results with detailed scores

### Command Line Mode

```bash
python main.py "find midcap stocks with reversal signals"
```

### Demo & Documentation

```bash
python TEST_DEMO.py
```

This shows usage examples and feature overview.

## Architecture

### Project Structure

```
Screener/
├── main.py                      # Interactive CLI interface
├── agents/
│   ├── style_agent.py          # Parse user intent
│   ├── quant_agent_logic.py    # Generate screening logic
│   └── reasoning_agent.py      # Advanced reasoning (optional)
├── data/
│   └── fetcher.py              # yfinance data fetching
├── quant/
│   ├── executor.py             # Execute screening contracts
│   └── features.py             # Technical pattern detection
├── schemas/
│   ├── intent.py               # Intent data structure
│   ├── quant_contract.py       # Screening contract schema
│   └── screener_output.py      # Output format
├── utils/
│   ├── llm.py                  # LLM integration
│   └── safe_llm.py             # Safe LLM wrapper
└── requirements.txt             # Dependencies
```

### Data Flow

```
User Input
    ↓
Style Agent (Parse Intent)
    ↓
Quant Agent (Generate Filters)
    ↓
Data Fetcher (yfinance)
    ↓
Technical Analyzer (Calculate Indicators)
    ↓
Pattern Detector (Reversal/Breakout Scores)
    ↓
Executor (Apply Filters & Rank)
    ↓
Display Results (Rich Table)
```

## Configuration

### Customize Stock Universe

Edit `main.py` line ~150 to change the stock symbols:

```python
stock_universe = [
    'RELIANCE.NS',  # NSE (India)
    'INFY.NS',
    'AAPL',         # US stocks
    'MSFT',
    # Add your favorites...
]
```

### Adjust Scoring Weights

Edit `quant/features.py` to modify pattern weights:

```python
# Line ~130 - Change composite score calculation
composite_score = (reversal['reversal_score'] * 0.4 + 
                  breakout['breakout_score'] * 0.6)
```

### Filter by Minimum Score

Edit `main.py` to change minimum score threshold:

```python
# Line ~170
results = screen_for_patterns(
    data_provider,
    symbols=stock_universe,
    pattern_type="both",
    min_score=50  # Adjust threshold here (0-100)
)
```

## Output Example

```
╭──────────────────────────────────────────────────────────────────────╮
│                    Screener Results                                  │
├─────────┬──────────────┬─────────────┬────────┬────────┬──────┬────┤
│ Symbol  │ Name         │ Market Cap  │ Price  │ Score  │ Rev  │ Brk│
├─────────┼──────────────┼─────────────┼────────┼────────┼──────┼────┤
│ RELIANCE│ Reliance Ind.│ large_cap   │ $2850  │ 72.5   │ 65.0 │ 75 │
│ INFY    │ Infosys      │ large_cap   │ $1450  │ 68.0   │ 60.0 │ 70 │
│ TCS     │ Tata Consult │ large_cap   │ $3520  │ 45.0   │ 40.0 │ 48 │
└─────────┴──────────────┴─────────────┴────────┴────────┴──────┴────┘

Found 3 matching stocks
```

## Advanced Features

### Pattern Signals

The screener provides detailed signals for each pattern:

**For Reversal:**
- RSI Oversold (<30)
- Price at lower Bollinger Band
- MACD Bullish Crossover
- Bounce from recent support
- Above-average volume

**For Breakout:**
- Price above SMA 20 > SMA 50
- Breaking upper Bollinger Band
- Strong volume breakout (>1.5x avg)
- Near 52-week high
- Room to run (RSI < 70)

### Score Interpretation

| Composite Score | Recommendation | Action |
|---|---|---|
| 75-100 | Strong Setup | High conviction entry |
| 60-74 | Good Setup | Solid risk/reward |
| 45-59 | Monitor | Watch for confirmation |
| 0-44 | Weak Setup | Skip or wait |

## Data Sources

- **Price & Volume**: yfinance (real-time)
- **Market Data**: yfinance ticker info
- **Technical Analysis**: Custom Python calculations

## Dependencies

- `yfinance` - Stock market data
- `pandas` - Data manipulation
- `numpy` - Numerical computations
- `ta` - Technical analysis indicators
- `langchain` - LLM integration
- `langgraph` - Agent workflow
- `rich` - Beautiful CLI output
- `pydantic` - Data validation

## Performance Tips

1. **Cache Market Data**: The screener caches data to avoid repeated API calls
2. **Use Smaller Universe**: Start with 20-30 stocks for faster results
3. **Adjust Time Period**: Use "1mo" instead of "6mo" for faster execution
4. **Filter by Market Cap**: Pre-filter to reduce screening time

## Limitations & Disclaimers

⚠️ **Important**: 
- This is a **screening tool**, not investment advice
- Past patterns don't guarantee future results
- Always do your own research before trading
- Consider multiple signals before making decisions
- Risk management is essential (stops, position sizing)

## Future Enhancements

- [ ] Multiple timeframe analysis (daily, weekly, monthly)
- [ ] Support for US, European stock markets
- [ ] Real-time alert system
- [ ] Portfolio tracking integration
- [ ] Risk metrics (Sharpe ratio, drawdown analysis)
- [ ] Machine learning pattern recognition
- [ ] Database persistence for historical analysis

## License

MIT License

## Support

For issues or feature requests, please raise an issue in the repository.

---

**Happy Screening!** 📈🚀
