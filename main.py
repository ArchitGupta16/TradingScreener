"""
Interactive Stock Screener with Trend Reversal & Breakout Detection.
Uses LLM to interpret user intent, then screens with technical analysis.
"""
import sys
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt

from agents.style_agent import style_agent
from agents.quant_agent_logic import screen_logic_agent
from quant.executor import execute_quant_contract, screen_for_patterns
from data.fetcher import ScreenerDataProvider
from stock_universe.stock_symbols import read_stock_symbols
from configs.config import config as omegacnf

console = Console()
config = omegacnf()

def format_results(df: pd.DataFrame, limit: int = 10) -> str:
    """Format screening results for display."""
    if df.empty:
        return "No stocks matched the criteria."
    
    df_display = df.head(limit).copy()
    
    # Create rich table 
    # Display only symbol, name, market cap, recommendation, scores
    table = Table(title="Screener Results")
    table.add_column("Symbol", style="cyan", no_wrap=True)
    table.add_column("Name", style="white")
    table.add_column("Market Cap", style="magenta")
    # table.add_column("Price", style="green")
    table.add_column("Composite\nScore", style="yellow")
    # table.add_column("Reversal", style="blue")
    # table.add_column("Breakout", style="blue")
    table.add_column("Recommendation", style="bold red")
    
    for _, row in df_display.iterrows():
        symbol = str(row.get('symbol', 'N/A'))
        name = str(row.get('name', 'N/A'))[:20]
        market_cap = str(row.get('market_cap_category', 'N/A'))
        # price = f"${row.get('current_price', 0):.2f}"
        # composite = f"{row.get('composite_score', 0):.1f}"
        # reversal = f"{row.get('reversal_score', 0):.1f}"
        # breakout = f"{row.get('breakout_score', 0):.1f}"
        rec = str(row.get('recommendation', 'N/A'))
        
        table.add_row(symbol, name, market_cap, rec)
    
    # Print table to string
    from io import StringIO
    string_io = StringIO()
    temp_console = Console(file=string_io, width=120)
    temp_console.print(table)
    return string_io.getvalue()


def interactive_screener():
    """Run interactive screening session."""
    console.print(Panel.fit(
        "[bold cyan]Stock Screener with AI[/]\nTrend Reversal & Breakout Detection",
        border_style="green"
    ))
    
    # data_provider = ScreenerDataProvider()
    
    while True:
        console.print("\n[bold yellow]Select Mode:[/] 1. AI-assisted  2. Manual")
        user_mode = Prompt.ask("Mode", choices=["AI", "Manual"], default="Manual")
        
        console.print("\n[bold yellow]Enter your screening request[/] (or 'quit' to exit):")
        user_input = Prompt.ask("You").strip()
        
        if user_input.lower() in ['quit', 'exit', 'q']:
            console.print("[yellow]Goodbye![/]")
            break
        
        if not user_input:
            console.print("[red]Please enter a valid request.[/]")
            continue
        
        try:
            console.print("\n[bold blue]Processing your request...[/]")
            
            # Step 1: Parse intent with LLM
            console.print("1️⃣  Interpreting intent...")
            intent = style_agent(user_input)
            console.print(f"   → Style: {intent.style}, Market: {intent.market}")
            
            # Extract market cap category from request
            limit = intent.count if intent.count else 10
            market_cap_pref = intent.cap.lower() if intent.cap else None
            pattern_type = "both"  # Default
            
            if "reversal" in user_input.lower():
                pattern_type = "reversal"
            elif "breakout" in user_input.lower():
                pattern_type = "breakout"
            
            # Step 3: Build stock universe based on request
            console.print("3️⃣  Fetching market data...")
            path = f"D://Projects//Screener//stock_universe//{intent.market}.txt"
            stock_universe = read_stock_symbols(
                                        file_path=path,
                                        symbol_column="ExchangeCode"   # or "ScripName"
                                    )

            # Screen for patterns
            results, all_stock_df = screen_for_patterns(
                symbols=stock_universe,
                pattern_type=pattern_type,
                queries=config.queries,
                min_score=50
            )
            
            if user_mode == "Manual":
                console.print("   → Manual mode selected. Skipping LLM-based screening.")
                if market_cap_pref and not results.empty:
                # Filter by market cap if specified (only if column exists)
                # if market_cap_pref and not results.empty and 'market_cap_category' in results.columns:
                    # results = results[results['market_cap_category'] == market_cap_pref]
                    
                    #print results
                    results = pd.DataFrame([stock.model_dump() for stock in results.stocks])

                    # Limit results
                    if len(results) > limit:
                        results = results.head(limit)
                    
                    # Step 4: Display results
                    console.print("\n4️⃣  [bold green]Results:[/]")
                    if not results.empty:
                        console.print(format_results(results, limit=limit))
                        console.print(f"\n[bold]Found {len(results)} matching stocks[/]")
                    else:
                        console.print("[yellow]No stocks matched your criteria. Try adjusting filters.[/]")
                
            # screen logic execution
            if user_mode == "AI":
                console.print("4. Executing screening logic using LLM...")
                results = screen_logic_agent(intent, all_stock_df[['symbol', 'rsi', 'atr_percent', 'volume_ratio']])
                console.print("\n[bold green]Results:[/]")
                if not results.empty:
                    console.print(format_results(results, limit=limit))
                    console.print(f"\n[bold]Found {len(results)} matching stocks[/]")
        
        except Exception as e:
            console.print(f"[red]Error: {str(e)}[/]")
            import traceback
            traceback.print_exc()


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        # Non-interactive mode: process single query from command line
        query = " ".join(sys.argv[1:])
        print(f"Query: {query}")
        
        data_provider = ScreenerDataProvider()
        results = screen_for_patterns(
            data_provider,
            symbols=[
                'RELIANCE.NS', 'INFY.NS', 'TCS.NS', 'HDFCBANK.NS', 'ICICIBANK.NS',
                'SBIN.NS', 'MARUTI.NS', 'BHARTIARTL.NS', 'HDFC.NS', 'WIPRO.NS',
            ],
            pattern_type="both",
            min_score=50
        )
        
        if not results.empty:
            print(f"\nFound {len(results)} matching stocks:\n")
            print(results.to_string(index=False))
        else:
            print("No results found.")
    else:
        # Interactive mode
        interactive_screener()


if __name__ == "__main__":
    main()

