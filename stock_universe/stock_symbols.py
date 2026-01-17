import pandas as pd
from typing import List, Optional

def read_stock_symbols(
    file_path: str,
    symbol_column: str = "ShortName",
    series_filter: Optional[str] = "EQ"
):
    """
    Reads a CSV-formatted .txt file and returns NSE stock symbols.

    Parameters
    ----------
    file_path : str
        Path to the .txt file
    symbol_column : str
        Column name containing stock symbols (default: ShortName)
    series_filter : str | None
        Filter by Series (e.g. 'EQ'). Set None to disable.
    return_df : bool
        If True, return (symbols, filtered_df)

    Returns
    -------
    list[str] or (list[str], pd.DataFrame)
    """

    # Robust read (handles BOM + bad quotes)
    df = pd.read_csv(
        file_path,
        encoding="utf-8-sig",
        quotechar='"',
        low_memory=False
    )

    # Clean column names
    df.columns = (
        df.columns
        .str.replace('"', '', regex=False)
        .str.strip()
    )

    if symbol_column not in df.columns:
        raise ValueError(
            f"Column '{symbol_column}' not found. "
            f"Available columns: {list(df.columns)}"
        )

    # Optional equity filter
    if series_filter and "Series" in df.columns:
        df = df[df["Series"].astype(str).str.upper() == series_filter]

    # Filter stocks having less than 6 months of data
    # df['DateOfListing'] = pd.to_datetime(df['DateOfListing'], errors='coerce')
    # six_months_ago = pd.Timestamp.now() - pd.DateOffset(months=6)
    # df = df[df['DateOfListing'] <= six_months_ago]

    # Clean symbols
    symbols = (
        df[symbol_column]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
        .unique()
        .tolist()
    )

    return symbols
